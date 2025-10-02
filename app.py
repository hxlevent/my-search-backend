# app.py — 조회 API + CSV Replace 업로드(임시 스왑) + 인코딩/헤더 정규화

import os, io, csv, time
from flask import Flask, jsonify, request, Response
from flask_cors import CORS
from pymongo import MongoClient, InsertOne
from pymongo.errors import BulkWriteError
from pymongo.collation import Collation
from bson import json_util

app = Flask(__name__)
# 운영에서는 origin 제한을 권장: CORS(app, resources={r"/*": {"origins": ["https://<your-gh>.github.io"]}})
CORS(app)

# ===== Mongo 연결 =====
CONNECTION_STRING = os.environ.get("MONGO_URI")
DB_NAME = os.environ.get("MONGO_DB", "my_database")
COLLECTION_NAME = os.environ.get("MONGO_COLLECTION", "my_collection")

client = None
collection = None
if CONNECTION_STRING:
    try:
        client = MongoClient(CONNECTION_STRING, serverSelectionTimeoutMS=5000)
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]
        client.admin.command("ping")
        print("MongoDB 연결 성공!")
    except Exception as e:
        print(f"MongoDB 연결 실패: {e}")
        client = None
        collection = None
else:
    print("환경변수 MONGO_URI가 설정되지 않았습니다.")


def dumps_json(obj):
    """ObjectId/Datetime 안전 직렬화 응답"""
    return Response(json_util.dumps(obj, ensure_ascii=False), mimetype="application/json")


# ===== 기본/헬스 =====
@app.get("/")
def home():
    return "API 서버가 정상 작동 중입니다." if client else "DB 연결 실패"

@app.get("/healthz")
def healthz():
    try:
        client.admin.command("ping")
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


# ===== 조회 API =====
@app.get("/search")
def search_by_unique_id():
    """
    쿼리:
      - unique_id (또는 id) : 필수 (프론트에서 대문자 변환하여 호출 권장)
      - page / limit        : 선택 (기본 1, 200 / 최대 1000)
    응답: [{unique_id, 날짜, 시간, 인증샷 시리얼넘버}] (ts로 정렬)
    """
    if client is None or collection is None:
        return jsonify({"ok": False, "error": "DB 연결 실패"}), 500

    unique_id = request.args.get("unique_id") or request.args.get("id")
    if not unique_id:
        return dumps_json([])

    try:
        page = max(int(request.args.get("page", 1)), 1)
    except Exception:
        page = 1
    try:
        limit = min(max(int(request.args.get("limit", 200)), 1), 1000)
    except Exception:
        limit = 200
    skip = (page - 1) * limit

    pipeline = [
        {"$match": {"unique_id": unique_id}},
        # '0:39' 같은 한 자리 시간을 '00:39'로 패딩 후 날짜+시간을 ts로 파싱
        {"$addFields": {
            "_padded_time": {
                "$cond": [
                    {"$lt": [{"$strLenCP": "$시간"}, 5]},
                    {"$concat": ["0", "$시간"]},
                    "$시간"
                ]
            }
        }},
        {"$addFields": {
            "ts": {
                "$dateFromString": {
                    "dateString": {"$concat": ["$날짜", " ", "$_padded_time"]},
                    "format": "%Y.%m.%d %H:%M",
                    "onError": None,
                    "onNull": None
                }
            }
        }},
        {"$sort": {"ts": 1, "_id": 1}},
        {"$skip": skip},
        {"$limit": limit},
        {"$project": {
            "_id": 0,
            "unique_id": 1,
            "날짜": 1,
            "시간": 1,
            "인증샷 시리얼넘버": 1
        }}
    ]
    docs = list(collection.aggregate(pipeline))
    return dumps_json(docs)


@app.get("/search-serial")
def search_by_serial():
    """시리얼 정확 일치 검색"""
    if client is None or collection is None:
        return jsonify({"ok": False, "error": "DB 연결 실패"}), 500

    serial = request.args.get("serial")
    if not serial:
        return dumps_json([])

    projection = {"_id": 0, "unique_id": 1, "날짜": 1, "시간": 1, "인증샷 시리얼넘버": 1}
    docs = list(
        collection.find({"인증샷 시리얼넘버": serial}, projection)
                  .sort([("unique_id", 1), ("날짜", 1), ("시간", 1)])
    )
    return dumps_json(docs)


# ===== 관리자 업로드(Replace) =====

REQUIRED_HEADERS = {"unique_id", "날짜", "시간", "인증샷 시리얼넘버"}

def _normalize_row_strict(row: dict) -> dict:
    """필수 4컬럼만 사용, 공백 제거, unique_id 대문자화"""
    return {
        "unique_id": (row["unique_id"] or "").strip().upper(),
        "날짜": (row["날짜"] or "").strip(),
        "시간": (row["시간"] or "").strip(),
        "인증샷 시리얼넘버": (row["인증샷 시리얼넘버"] or "").strip(),
    }

def _ensure_indexes(col, unique_serial: bool = False):
    """조회 성능 및(선택) 시리얼 고유 보장"""
    col.create_index("unique_id")
    if unique_serial:
        col.create_index("인증샷 시리얼넘버", unique=True)
    else:
        col.create_index("인증샷 시리얼넘버")

def _open_csv_reader_with_fallback(file_storage, preferred_encoding="utf-8"):
    """
    UTF-8/UTF-8-SIG/CP949/EUC-KR 순으로 디코딩 시도.
    성공하는 csv.DictReader와 사용 인코딩을 반환.
    """
    order = []
    if preferred_encoding:
        pe = preferred_encoding.lower()
        if pe == "utf-8":
            order = ["utf-8", "utf-8-sig", "cp949", "euc-kr"]
        elif pe == "cp949":
            order = ["cp949", "utf-8", "utf-8-sig", "euc-kr"]
        else:
            order = [pe, "utf-8", "utf-8-sig", "cp949", "euc-kr"]
    else:
        order = ["utf-8", "utf-8-sig", "cp949", "euc-kr"]

    last_error = None
    for enc in order:
        try:
            file_storage.stream.seek(0)
            text = io.TextIOWrapper(file_storage.stream, encoding=enc, newline="")
            reader = csv.DictReader(text)
            _ = reader.fieldnames  # 헤더 로드해 디코딩 확인
            return reader, enc
        except UnicodeDecodeError as e:
            last_error = e
            continue
    raise last_error or UnicodeDecodeError("encoding", b"", 0, 1, "unknown")

def _normalize_headers(raw_fields):
    """
    헤더에서 BOM/앞뒤 공백 제거.
    ['\\ufeffunique_id',' 날짜 '] -> ['unique_id','날짜']
    """
    raw_fields = raw_fields or []
    norm = [ (f or "").strip().lstrip("\ufeff") for f in raw_fields ]
    return norm


@app.get("/admin")
def admin_form():
    return """
<!doctype html>
<meta charset="utf-8"><title>CSV 업로드 (Replace)</title>
<style>
  body{font-family:sans-serif;padding:24px;max-width:720px;margin:0 auto}
  input,button,select{font-size:16px;padding:6px}
</style>
<h2>CSV 업로드 → MongoDB (전체 교체: Replace)</h2>
<form action="/admin/upload-csv-replace" method="post" enctype="multipart/form-data">
  <div>관리자 토큰: <input type="password" name="token" required></div><br>
  <div>CSV 파일: <input type="file" name="file" accept=".csv,text/csv" required></div><br>
  <div>인코딩:
    <select name="encoding">
      <option value="utf-8" selected>UTF-8</option>
      <option value="cp949">CP949(엑셀-한글)</option>
    </select>
  </div><br>
  <div>
    시리얼 고유 인덱스(선택):
    <label><input type="checkbox" name="unique_serial" value="1"> "인증샷 시리얼넘버"를 unique 인덱스로 생성</label>
  </div><br>
  <button type="submit">업로드 시작 (Replace)</button>
</form>
<p>CSV 헤더는 <code>unique_id, 날짜, 시간, 인증샷 시리얼넘버</code> 4개를 사용합니다(순서 무관).<br/>
BOM이나 공백이 있어도 자동 정리합니다.</p>
"""


@app.post("/admin/upload-csv-replace")
def upload_csv_replace():
    # 권한
    token = (request.form.get("token") or request.headers.get("X-Admin-Token") or "").strip()
    if token != os.environ.get("ADMIN_TOKEN", ""):
        return jsonify({"ok": False, "error": "Unauthorized"}), 401
    if client is None or collection is None:
        return jsonify({"ok": False, "error": "DB 연결 실패"}), 500

    # 입력
    file = request.files.get("file")
    if not file:
        return jsonify({"ok": False, "error": "파일이 없습니다."}), 400
    encoding = (request.form.get("encoding") or "utf-8").lower()
    unique_serial = request.form.get("unique_serial") == "1"

    db = collection.database
    target_name = collection.name
    temp_name   = f"{target_name}_tmp_{int(time.time())}"
    backup_name = f"{target_name}_bak_{int(time.time())}"

    temp_col = db[temp_name]
    inserted = skipped = 0
    ops = []
    batch_size = 1000

    try:
        # 1) 인코딩 자동 시도 + 헤더 정규화
        reader, used_enc = _open_csv_reader_with_fallback(file, preferred_encoding=encoding)
        norm_fields = _normalize_headers(reader.fieldnames)
        if not REQUIRED_HEADERS.issubset(set(norm_fields)):
            return jsonify({
                "ok": False,
                "error": "CSV 헤더 불일치",
                "required": sorted(REQUIRED_HEADERS),
                "got": norm_fields,
                "encoding_used": used_enc
            }), 400

        # 정규화된 헤더로 다시 Reader 구성 (BOM/공백 제거된 헤더 강제 적용)
        file.stream.seek(0)
        text_raw = io.TextIOWrapper(file.stream, encoding=used_enc, newline="")
        header_line = text_raw.readline()  # 원본 헤더 라인 건너뛰기
        rest = text_raw.read()
        fixed_header = ",".join(norm_fields) + "\n"
        fixed_text = io.StringIO(fixed_header + rest)
        reader = csv.DictReader(fixed_text)

        # 2) 임시 컬렉션 적재 ( streaming + bulk )
        for row in reader:
            # 다른 여분 컬럼은 무시하고 필수 4개만 사용
            safe_row = {
                "unique_id": row.get("unique_id", ""),
                "날짜": row.get("날짜", ""),
                "시간": row.get("시간", ""),
                "인증샷 시리얼넘버": row.get("인증샷 시리얼넘버", "")
            }
            doc = _normalize_row_strict(safe_row)
            # 필수값 검증(빈 값은 스킵)
            if not doc["unique_id"] or not doc["날짜"] or not doc["시간"]:
                skipped += 1
                continue
            ops.append(InsertOne(doc))
            if len(ops) >= batch_size:
                res = temp_col.bulk_write(ops, ordered=False)
                inserted += res.inserted_count
                ops.clear()
        if ops:
            res = temp_col.bulk_write(ops, ordered=False)
            inserted += res.inserted_count
            ops.clear()

        # 3) 임시 컬렉션 인덱스
        _ensure_indexes(temp_col, unique_serial=unique_serial)

        # 4) 스왑: 기존→백업, 임시→본컬렉션
        try:
            db[target_name].rename(backup_name, dropTarget=True)
        except Exception:
            pass  # 기존이 없으면 무시
        temp_col.rename(target_name, dropTarget=True)

        return jsonify({
            "ok": True,
            "mode": "replace",
            "inserted": inserted,
            "skipped": skipped,
            "backup": backup_name,
            "encoding_used": used_enc
        })

    except BulkWriteError as bwe:
        return jsonify({"ok": False, "error": "Bulk write error", "detail": bwe.details}), 500
    except UnicodeDecodeError as e:
        return jsonify({"ok": False, "error": str(e)}), 400
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


if __name__ == "__main__":
    # 로컬 테스트용 (Render에서는 gunicorn 사용)
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))
