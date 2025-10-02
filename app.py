# app.py — Replace(전체 교체) + 조회 API

from flask import Flask, jsonify, request, Response
from flask_cors import CORS
from pymongo import MongoClient, InsertOne
from pymongo.errors import BulkWriteError
from bson import json_util
import os, io, csv, time

app = Flask(__name__)
# 운영시 origin 제한 권장: CORS(app, resources={r"/*": {"origins": ["https://<your-gh>.github.io"]}})
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
    return Response(json_util.dumps(obj, ensure_ascii=False), mimetype="application/json")


# ===== 조회 API =====
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

# unique_id 검색 (프론트에서 대문자 변환하여 호출)
from pymongo.collation import Collation
@app.get("/search")
def search_by_unique_id():
    if client is None or collection is None:
        return jsonify({"ok": False, "error": "DB 연결 실패"}), 500

    unique_id = request.args.get("unique_id") or request.args.get("id")
    if not unique_id:
        return dumps_json([])

    # 페이지네이션
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

# 시리얼 검색(정확 일치)
@app.get("/search-serial")
def search_by_serial():
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


# ===== 관리자: CSV Replace (전체 교체) =====

REQUIRED_HEADERS = {"unique_id", "날짜", "시간", "인증샷 시리얼넘버"}

def _normalize_row_strict(row: dict) -> dict:
    # 공백 제거, unique_id는 대문자화
    return {
        "unique_id": (row["unique_id"] or "").strip().upper(),
        "날짜": (row["날짜"] or "").strip(),
        "시간": (row["시간"] or "").strip(),
        "인증샷 시리얼넘버": (row["인증샷 시리얼넘버"] or "").strip(),
    }

def _ensure_indexes(col, unique_serial=False):
    # 조회 성능용 인덱스
    col.create_index("unique_id")
    if unique_serial:
        # 진짜 고유 보장(중복 있으면 생성 실패 가능)
        col.create_index("인증샷 시리얼넘버", unique=True)
    else:
        col.create_index("인증샷 시리얼넘버")

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
<p>CSV 헤더는 <code>unique_id, 날짜, 시간, 인증샷 시리얼넘버</code> 네 가지를 정확히 사용해야 합니다. (순서는 무관)</p>
"""

@app.post("/admin/upload-csv-replace")
def upload_csv_replace():
    # 0) 권한
    token = (request.form.get("token") or request.headers.get("X-Admin-Token") or "").strip()
    if token != os.environ.get("ADMIN_TOKEN", ""):
        return jsonify({"ok": False, "error": "Unauthorized"}), 401
    if client is None or collection is None:
        return jsonify({"ok": False, "error": "DB 연결 실패"}), 500

    # 1) 입력
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
        # 2) 헤더 검증 + 스트리밍 적재
        text = io.TextIOWrapper(file.stream, encoding=encoding, newline="")
        reader = csv.DictReader(text)

        fieldset = set(reader.fieldnames or [])
        if REQUIRED_HEADERS - fieldset:
            return jsonify({
                "ok": False,
                "error": "CSV 헤더 불일치",
                "required": sorted(REQUIRED_HEADERS),
                "got": list(reader.fieldnames or [])
            }), 400

        for row in reader:
            doc = _normalize_row_strict(row)
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
            "backup": backup_name
        })

    except BulkWriteError as bwe:
        return jsonify({"ok": False, "error": "Bulk write error", "detail": bwe.details}), 500
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


if __name__ == "__main__":
    # Render 등에서는 gunicorn 사용 권장: gunicorn app:app --bind 0.0.0.0:$PORT
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))
