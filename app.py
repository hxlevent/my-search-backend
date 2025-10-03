# app.py — 검색 API + 관리자 CSV Replace(비동기) + 진행률 폴링

import os, io, csv, time, uuid, threading
from flask import Flask, jsonify, request, Response
from flask_cors import CORS
from bson import json_util
from pymongo import MongoClient, InsertOne
from pymongo.write_concern import WriteConcern

# ----------------------------
# Flask / CORS
# ----------------------------
app = Flask(__name__)
# 운영에서는 origins를 GitHub Pages 도메인으로 제한 권장
# CORS(app, resources={r"/*": {"origins": ["https://<your-gh>.github.io"]}})
CORS(app)

# ----------------------------
# Mongo 연결 설정
# ----------------------------
CONNECTION_STRING = os.environ.get("MONGO_URI")
DB_NAME = os.environ.get("MONGO_DB", "my_database")
COLLECTION_NAME = os.environ.get("MONGO_COLLECTION", "my_collection")

client = None
collection = None

if CONNECTION_STRING:
    try:
        client = MongoClient(
            CONNECTION_STRING,
            serverSelectionTimeoutMS=5000,
            connectTimeoutMS=20000,
            socketTimeoutMS=600000,  # 긴 업로드 대비 10분
            retryWrites=True,
        )
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

# ----------------------------
# 공통 유틸
# ----------------------------
def dumps_json(obj):
    """ObjectId/Datetime 안전 직렬화 응답"""
    return Response(json_util.dumps(obj, ensure_ascii=False), mimetype="application/json")

# 진행률 저장소 (인메모리)
JOBS = {}  # job_id -> dict(status, phase, total_rows, processed_rows, inserted, skipped, started_at, ended_at, error, encoding)

def _update_job(job_id, **kw):
    if job_id in JOBS:
        JOBS[job_id].update(kw)

def _count_csv_lines_fast(path, encoding):
    """헤더 1줄 제외한 데이터 라인 수(대략치)"""
    cnt = 0
    with open(path, "r", encoding=encoding, newline="") as f:
        for _ in f:
            cnt += 1
    return max(0, cnt - 1)

REQUIRED_HEADERS = {"unique_id", "날짜", "시간", "인증샷 시리얼넘버"}

def _normalize_headers_map(raw_fields):
    """
    헤더 정규화 맵: 원본키 -> 정규키
    '\ufeffunique_id' / ' 날짜 ' -> 'unique_id' / '날짜'
    """
    return { (f or ""): (f or "").strip().lstrip("\ufeff") for f in (raw_fields or []) }

# ----------------------------
# 헬스/루트
# ----------------------------
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

# ----------------------------
# 조회 API
# ----------------------------
@app.get("/search")
def search_by_unique_id():
    """
    쿼리:
      - unique_id (또는 id) : 필수 (프론트에서 대문자 변환하여 호출 권장)
      - page / limit        : 선택 (기본 1, 200 / 최대 1000)
    응답: [{unique_id, 날짜, 시간, 인증샷 시리얼넘버}] (날짜+시간 정렬)
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

    # 날짜/시간을 ts로 파싱해 정렬 (0:39 → 00:39 패딩)
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

@app.get("/search-serial")
def search_by_serial():
    """시리얼 정확 일치 검색 (인덱스 없이도 동작; 데이터량 수준에서 충분)"""
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

# ----------------------------
# 관리자 페이지(UI) — 비동기 업로드 + 진행률 표시
# ----------------------------
@app.get("/admin")
def admin_form():
    return """
<!doctype html><meta charset="utf-8"><title>CSV 업로드(Replace)</title>
<style>
 body{font-family:sans-serif;padding:24px;max-width:720px;margin:0 auto}
 .bar{height:14px;background:#e5e7eb;border-radius:7px;overflow:hidden}
 .bar>i{display:block;height:100%;width:0;background:#38bdf8;transition:width .2s}
 .row{margin:8px 0}
 code{background:#f3f4f6;padding:2px 6px;border-radius:4px}
</style>
<h2>CSV 업로드 → MongoDB (전체 교체, 비동기)</h2>
<div class="row">관리자 토큰<br/><input id="tok" type="password" style="width:100%"></div>
<div class="row">CSV 파일(.csv 또는 .csv.gz)<br/><input id="file" type="file" accept=".csv,text/csv,.gz"></div>
<div class="row">인코딩<br/><select id="enc"><option>utf-8</option><option>cp949</option></select></div>
<button onclick="startUpload()">업로드 시작</button>
<div id="prog" style="display:none;margin-top:16px">
  <div>상태: <b id="phase">준비</b> · 진행: <b id="pct">0%</b></div>
  <div class="bar"><i id="bar"></i></div>
  <div class="row">처리 <span id="proc">0</span>/<span id="total">0</span> · 삽입 <span id="ins">0</span> · 스킵 <span id="skp">0</span></div>
  <div class="row">경과 <span id="elap">0</span>s · 남은예상 <span id="eta">-</span>s</div>
  <div id="err" style="color:#b91c1c"></div>
</div>
<script>
let jobId=null, poll=null;
function qs(i){return document.getElementById(i)}
async function startUpload(){
  const file = qs('file').files[0];
  const tok  = qs('tok').value.trim();
  const enc  = qs('enc').value;
  if(!file||!tok){alert('토큰/파일을 입력하세요');return;}
  const fd = new FormData();
  fd.append('token', tok); fd.append('encoding', enc); fd.append('file', file);
  const res = await fetch('/admin/upload-start',{method:'POST',body:fd});
  const j = await res.json();
  if(!j.ok){alert('실패: '+(j.error||'unknown'));return;}
  jobId = j.job_id; qs('prog').style.display='block';
  poll = setInterval(update, 1000);
}
async function update(){
  const r = await fetch('/admin/job-status?id='+jobId);
  const j = await r.json();
  if(!j.ok){clearInterval(poll); qs('err').textContent=j.error||'오류'; return;}
  const jb = j.job;
  const pct = jb.total_rows ? Math.floor(jb.processed_rows*100/jb.total_rows) : 0;
  qs('phase').textContent=jb.phase; qs('pct').textContent=pct+'%';
  qs('bar').style.width=pct+'%'; qs('proc').textContent=jb.processed_rows;
  qs('total').textContent=jb.total_rows; qs('ins').textContent=jb.inserted;
  qs('skp').textContent=jb.skipped; qs('elap').textContent=jb.elapsed_secs;
  qs('eta').textContent=(jb.eta_secs??'-');
  if(jb.status==='done'||jb.status==='error'){clearInterval(poll);}
  if(jb.status==='error'){qs('err').textContent=jb.error||'에러';}
}
</script>
"""

# ----------------------------
# 업로드 시작 (비동기 잡 생성)
# ----------------------------
@app.post("/admin/upload-start")
def admin_upload_start():
    token = (request.form.get("token") or request.headers.get("X-Admin-Token") or "").strip()
    if token != os.environ.get("ADMIN_TOKEN", ""):
        return jsonify({"ok": False, "error": "Unauthorized"}), 401
    if client is None or collection is None:
        return jsonify({"ok": False, "error": "DB 연결 실패"}), 500

    file = request.files.get("file")
    if not file:
        return jsonify({"ok": False, "error": "파일이 없습니다."}), 400

    enc = (request.form.get("encoding") or "utf-8").lower()

    # /tmp 로 저장 (비동기 처리를 위해)
    job_id = uuid.uuid4().hex[:12]
    tmp_path = f"/tmp/upload_{job_id}.csv"
    file.save(tmp_path)

    # 인코딩 간단 검증/보정
    try:
        with open(tmp_path, "r", encoding=enc, newline="") as tf:
            _ = tf.readline()
    except UnicodeDecodeError:
        for cand in ["utf-8", "utf-8-sig", "cp949", "euc-kr"]:
            try:
                with open(tmp_path, "r", encoding=cand, newline="") as tf:
                    _ = tf.readline()
                enc = cand
                break
            except UnicodeDecodeError:
                continue

    total = _count_csv_lines_fast(tmp_path, enc)

    JOBS[job_id] = {
        "status": "running",
        "phase": "parsing",
        "total_rows": total,
        "processed_rows": 0,
        "inserted": 0,
        "skipped": 0,
        "started_at": time.time(),
        "ended_at": None,
        "error": None,
        "encoding": enc,
    }

    threading.Thread(target=_run_replace_job, args=(job_id, tmp_path, enc), daemon=True).start()
    return jsonify({"ok": True, "job_id": job_id, "total_rows": total, "encoding": enc})

# ----------------------------
# 진행 상태 조회
# ----------------------------
@app.get("/admin/job-status")
def admin_job_status():
    job_id = request.args.get("id", "")
    job = JOBS.get(job_id)
    if not job:
        return jsonify({"ok": False, "error": "no such job"}), 404

    elapsed = time.time() - job["started_at"] if job["started_at"] else 0
    processed = max(1, job["processed_rows"])
    rate = processed / max(1, elapsed)
    remaining = max(0, job["total_rows"] - processed)
    eta = int(remaining / rate) if rate > 0 else None

    out = dict(job)
    out["elapsed_secs"] = int(elapsed)
    out["eta_secs"] = eta
    return jsonify({"ok": True, "job": out})

# ===== 관리자 요약 API =====
# GET /admin/summary?token=...&page=1&limit=100&q=AA
@app.get("/admin/summary")
def admin_summary():
    token = (request.args.get("token") or request.headers.get("X-Admin-Token") or "").strip()
    if token != os.environ.get("ADMIN_TOKEN", ""):
        return jsonify({"ok": False, "error": "Unauthorized"}), 401
    if client is None or collection is None:
        return jsonify({"ok": False, "error": "DB 연결 실패"}), 500

    # 페이지네이션
    try:
        page = max(int(request.args.get("page", 1)), 1)
    except Exception:
        page = 1
    try:
        limit = min(max(int(request.args.get("limit", 100)), 1), 1000)
    except Exception:
        limit = 100
    skip = (page - 1) * limit

    # 접두어 필터(대문자)
    q = (request.args.get("q") or "").strip().upper()
    uid_filter = {"unique_id": {"$regex": f"^{q}"}} if q else {}

    # --- Step 1) 페이지에 들어갈 unique_id만 선(先) 추출 ---
    # 큰 컬렉션 전체를 매번 group하지 않도록, ID 목록만 먼저 뽑는다.
    ids_stage = []
    if uid_filter:
        ids_stage.append({"$match": uid_filter})
    ids_stage += [
        {"$group": {"_id": "$unique_id"}},      # distinct unique_id
        {"$sort": {"_id": 1}},
        {"$skip": skip},
        {"$limit": limit},
        {"$project": {"_id": 0, "unique_id": "$_id"}}
    ]
    page_ids = [d["unique_id"] for d in collection.aggregate(ids_stage)]
    if not page_ids:
        # total_unique 계산 (필터 적용) — group + count는 distinct보다 가벼움
        count_pipeline = []
        if uid_filter:
            count_pipeline.append({"$match": uid_filter})
        count_pipeline += [{"$group": {"_id": "$unique_id"}}, {"$count": "n"}]
        cnt_doc = next(iter(collection.aggregate(count_pipeline)), {"n": 0})
        return jsonify({
            "ok": True, "page": page, "limit": limit, "count": 0,
            "total_unique": cnt_doc.get("n", 0),
            "rows": []
        })

    # --- 총 unique_id 수 (필터 적용) ---
    count_pipeline = []
    if uid_filter:
        count_pipeline.append({"$match": uid_filter})
    count_pipeline += [{"$group": {"_id": "$unique_id"}}, {"$count": "n"}]
    cnt_doc = next(iter(collection.aggregate(count_pipeline)), {"n": None})
    total_unique = cnt_doc.get("n", None)

    # --- Step 2) 해당 페이지의 ID들만 집계 ---
    # 시간 필드가 비어있거나 형식이 다른 경우도 안전 처리
    agg = [
        {"$match": {"unique_id": {"$in": page_ids}}},
        {"$addFields": {"_t": {"$ifNull": ["$시간", "00:00"]}}},
        {"$addFields": {
            "_padded_time": {
                "$cond": [
                    {"$lt": [{"$strLenCP": "$_t"}, 5]},
                    {"$concat": ["0", "$_t"]},
                    "$_t"
                ]
            }
        }},
        {"$addFields": {
            "minOfDay": {
                "$let": {
                    "vars": {
                        "h": {"$ifNull": [{"$arrayElemAt": [{"$split": ["$_padded_time", ":" ]}, 0]}, "0"]},
                        "m": {"$ifNull": [{"$arrayElemAt": [{"$split": ["$_padded_time", ":" ]}, 1]}, "0"]},
                    },
                    "in": {
                        "$add": [
                            {"$multiply": [
                                {"$toInt": {"$cond": [{"$regexMatch": {"input": "$$h", "regex": r"^\d+$"}}, "$$h", "0"]}},
                                60
                            ]},
                            {"$toInt": {"$cond": [{"$regexMatch": {"input": "$$m", "regex": r"^\d+$"}}, "$$m", "0"]}}
                        ]
                    }
                }
            }
        }},
        {"$group": {
            "_id": "$unique_id",
            "c19": {"$sum": {"$cond": [{"$eq": ["$날짜", "2025.09.19"]}, 1, 0]}},
            "c20": {"$sum": {"$cond": [{"$eq": ["$날짜", "2025.09.20"]}, 1, 0]}},
            "c21": {"$sum": {"$cond": [{"$eq": ["$날짜", "2025.09.21"]}, 1, 0]}},
            "c22": {"$sum": {"$cond": [{"$eq": ["$날짜", "2025.09.22"]}, 1, 0]}},
            "c23": {"$sum": {"$cond": [{"$eq": ["$날짜", "2025.09.23"]}, 1, 0]}},
            "c24": {"$sum": {"$cond": [{"$eq": ["$날짜", "2025.09.24"]}, 1, 0]}},
            "c25_first": {"$sum": {"$cond": [
                {"$and": [
                    {"$eq": ["$날짜", "2025.09.25"]},
                    {"$lte": ["$minOfDay", 599]}
                ]}, 1, 0]}},
            "c_live": {"$sum": {"$cond": [
                {"$and": [
                    {"$eq": ["$날짜", "2025.09.25"]},
                    {"$gte": ["$minOfDay", 720]}
                ]}, 1, 0]}},
        }},
        {"$project": {
            "_id": 0,
            "unique_id": "$_id",
            "c19": 1, "c20": 1, "c21": 1, "c22": 1, "c23": 1, "c24": 1,
            "c25_first": 1, "c_live": 1
        }}
    ]
    rows = list(collection.aggregate(agg))

    # 페이지 ID 순서에 맞춰 정렬 + 파생값 계산
    idx = {u: i for i, u in enumerate(page_ids)}
    rows.sort(key=lambda r: idx.get(r["unique_id"], 1e9))
    for r in rows:
        r["pre_total"] = sum([r.get("c19",0), r.get("c20",0), r.get("c21",0),
                              r.get("c22",0), r.get("c23",0), r.get("c24",0),
                              r.get("c25_first",0)])
        r["perfect"] = all([
            r.get("c19",0)>0, r.get("c20",0)>0, r.get("c21",0)>0,
            r.get("c22",0)>0, r.get("c23",0)>0, r.get("c24",0)>0,
            r.get("c25_first",0)>0
        ])

    return jsonify({
        "ok": True,
        "page": page,
        "limit": limit,
        "count": len(rows),
        "total_unique": total_unique,
        "rows": rows
    })




from flask import stream_with_context

@app.get("/admin/summary-export")
def admin_summary_export():
    # 1) 인증
    token = (request.args.get("token") or request.headers.get("X-Admin-Token") or "").strip()
    if token != os.environ.get("ADMIN_TOKEN", ""):
        return jsonify({"ok": False, "error": "Unauthorized"}), 401
    if client is None or collection is None:
        return jsonify({"ok": False, "error": "DB 연결 실패"}), 500

    # 2) 접두어 필터(선택)
    q = (request.args.get("q") or "").strip().upper()
    uid_filter = {"unique_id": {"$regex": f"^{q}"}} if q else {}

    # 3) unique_id 전체를 메모리에 한 번에 담지 말고 cursor로 배치 처리
    #    group → sort 하면 distinct 역할 + 정렬 가능
    id_pipeline = []
    if uid_filter:
        id_pipeline.append({"$match": uid_filter})
    id_pipeline += [
        {"$group": {"_id": "$unique_id"}},
        {"$sort": {"_id": 1}},
        {"$project": {"_id": 0, "unique_id": "$_id"}}
    ]
    id_cursor = collection.aggregate(id_pipeline, allowDiskUse=True)

    def gen_rows_for_ids(id_batch):
        # 기존 요약 파이프라인에서 '해당 ID들만' 집계
        agg = [
            {"$match": {"unique_id": {"$in": id_batch}}},
            {"$addFields": {"_t": {"$ifNull": ["$시간", "00:00"]}}},
            {"$addFields": {
                "_padded_time": {
                    "$cond": [
                        {"$lt": [{"$strLenCP": "$_t"}, 5]},
                        {"$concat": ["0", "$_t"]},
                        "$_t"
                    ]
                }
            }},
            {"$addFields": {
                "minOfDay": {
                    "$let": {
                        "vars": {
                            "h": {"$ifNull": [{"$arrayElemAt": [{"$split": ["$_padded_time", ":" ]}, 0]}, "0"]},
                            "m": {"$ifNull": [{"$arrayElemAt": [{"$split": ["$_padded_time", ":" ]}, 1]}, "0"]},
                        },
                        "in": {
                            "$add": [
                                {"$multiply": [
                                    {"$toInt": {"$cond": [{"$regexMatch": {"input": "$$h", "regex": r"^\d+$"}}, "$$h", "0"]}},
                                    60
                                ]},
                                {"$toInt": {"$cond": [{"$regexMatch": {"input": "$$m", "regex": r"^\d+$"}}, "$$m", "0"]}}
                            ]
                        }
                    }
                }
            }},
            {"$group": {
                "_id": "$unique_id",
                "c19": {"$sum": {"$cond": [{"$eq": ["$날짜", "2025.09.19"]}, 1, 0]}},
                "c20": {"$sum": {"$cond": [{"$eq": ["$날짜", "2025.09.20"]}, 1, 0]}},
                "c21": {"$sum": {"$cond": [{"$eq": ["$날짜", "2025.09.21"]}, 1, 0]}},
                "c22": {"$sum": {"$cond": [{"$eq": ["$날짜", "2025.09.22"]}, 1, 0]}},
                "c23": {"$sum": {"$cond": [{"$eq": ["$날짜", "2025.09.23"]}, 1, 0]}},
                "c24": {"$sum": {"$cond": [{"$eq": ["$날짜", "2025.09.24"]}, 1, 0]}},
                "c25_first": {"$sum": {"$cond": [
                    {"$and": [
                        {"$eq": ["$날짜", "2025.09.25"]},
                        {"$lte": ["$minOfDay", 599]}
                    ]}, 1, 0]}},
                "c_live": {"$sum": {"$cond": [
                    {"$and": [
                        {"$eq": ["$날짜", "2025.09.25"]},
                        {"$gte": ["$minOfDay", 720]}
                    ]}, 1, 0]}},
            }},
            {"$project": {
                "_id": 0,
                "unique_id": "$_id",
                "c19": 1, "c20": 1, "c21": 1, "c22": 1, "c23": 1, "c24": 1,
                "c25_first": 1, "c_live": 1
            }},
            {"$sort": {"unique_id": 1}}
        ]
        for r in collection.aggregate(agg, allowDiskUse=True):
            pre_total = sum([r.get("c19",0), r.get("c20",0), r.get("c21",0),
                             r.get("c22",0), r.get("c23",0), r.get("c24",0), r.get("c25_first",0)])
            perfect = all([
                r.get("c19",0)>0, r.get("c20",0)>0, r.get("c21",0)>0,
                r.get("c22",0)>0, r.get("c23",0)>0, r.get("c24",0)>0,
                r.get("c25_first",0)>0
            ])
            yield [
                r["unique_id"], r.get("c19",0), r.get("c20",0), r.get("c21",0),
                r.get("c22",0), r.get("c23",0), r.get("c24",0),
                r.get("c25_first",0), r.get("c_live",0), pre_total, "O" if perfect else "X"
            ]

    def generate():
        # CSV 헤더
        header = ["unique_id","c19","c20","c21","c22","c23","c24","c25_first","c_live","pre_total","perfect"]
        yield ",".join(header) + "\n"

        batch = []
        batch_size = 1000  # 메모리/성능 밸런스. 2~5천으로 올려도 됨.
        for d in id_cursor:
            batch.append(d["unique_id"])
            if len(batch) >= batch_size:
                for row in gen_rows_for_ids(batch):
                    yield ",".join(map(str, row)) + "\n"
                batch.clear()
        if batch:
            for row in gen_rows_for_ids(batch):
                yield ",".join(map(str, row)) + "\n"

    resp = Response(stream_with_context(generate()), mimetype="text/csv; charset=utf-8")
    resp.headers["Content-Disposition"] = "attachment; filename=summary_all.csv"
    return resp









# ----------------------------
# 실제 Replace 작업 (백그라운드)
# ----------------------------
def _run_replace_job(job_id, path, encoding):
    try:
        db = collection.database
        target_name = collection.name
        temp_name   = f"{target_name}_tmp_{int(time.time())}_{job_id}"
        backup_name = f"{target_name}_bak_{int(time.time())}"

        # writeConcern: w=1(안전). 초고속 원하면 w=0로도 가능(실패 감지 어려움)
        temp_col = db.get_collection(temp_name, write_concern=WriteConcern(w=1))

        batch = []
        batch_size = 3000  # 데이터/네트워크 상황에 맞춰 2000~5000에서 조정
        inserted = skipped = processed = 0

        _update_job(job_id, phase="loading")

        with open(path, "r", encoding=encoding, newline="") as f:
            reader = csv.DictReader(f)
            fmap = _normalize_headers_map(reader.fieldnames)
            norm_set = set(fmap.values())
            if not REQUIRED_HEADERS.issubset(norm_set):
                raise ValueError(f"CSV 헤더 불일치: got={list(norm_set)}")

            # 원본 헤더 키 → 정규키 매핑 기반으로 값 추출
            def get(row, want):
                for k, v in fmap.items():
                    if v == want:
                        return row.get(k, "")
                return ""

            for row in reader:
                doc = {
                    "unique_id": get(row, "unique_id").strip().upper(),
                    "날짜": get(row, "날짜").strip(),
                    "시간": get(row, "시간").strip(),
                    "인증샷 시리얼넘버": get(row, "인증샷 시리얼넘버").strip(),
                }
                if not doc["unique_id"] or not doc["날짜"] or not doc["시간"]:
                    skipped += 1
                else:
                    batch.append(InsertOne(doc))
                    if len(batch) >= batch_size:
                        res = temp_col.bulk_write(batch, ordered=False)
                        inserted += res.inserted_count
                        batch.clear()
                processed += 1
                _update_job(job_id, processed_rows=processed, inserted=inserted, skipped=skipped)

        if batch:
            res = temp_col.bulk_write(batch, ordered=False)
            inserted += res.inserted_count
            batch.clear()
            _update_job(job_id, inserted=inserted)

        # 인덱스: unique_id만(시리얼 인덱스는 생성하지 않음)
        _update_job(job_id, phase="indexing")
        temp_col.create_index("unique_id")

        # 스왑: 기존 → 백업, 임시 → 타깃
        _update_job(job_id, phase="swapping")
        try:
            db[target_name].rename(backup_name, dropTarget=True)
        except Exception:
            pass
        db[temp_name].rename(target_name, dropTarget=True)

        _update_job(job_id, status="done", phase="done", ended_at=time.time())
    except Exception as e:
        _update_job(job_id, status="error", phase="error", error=str(e), ended_at=time.time())
    finally:
        try:
            os.remove(path)
        except Exception:
            pass

# ----------------------------
# 로컬 실행
# ----------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))
