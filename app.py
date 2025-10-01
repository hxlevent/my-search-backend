# app.py  — 완전 교체용
from flask import Flask, jsonify, request, Response
from flask_cors import CORS
from pymongo import MongoClient
from bson import json_util
import os

app = Flask(__name__)
# 운영에서는 origins에 GitHub Pages 도메인만 넣는 걸 추천:
# CORS(app, resources={r"/*": {"origins": ["https://<your-gh-username>.github.io"]}})
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


# ===== 유틸 =====
def dumps_json(obj):
    """ObjectId/Datetime 안전 직렬화"""
    return Response(json_util.dumps(obj, ensure_ascii=False), mimetype="application/json")


def build_ts_pipeline():
    """
    컬렉션의 '날짜' + '시간'을 합쳐 ts(datetime) 필드를 만드는 공통 파이프라인 조각.
    '0:39' 같은 한 자리 시를 '00:39'로 패딩해서 파싱합니다.
    """
    return [
        {"$addFields": {
            "_padded_time": {
                "$cond": [
                    {"$lt": [{"$strLenCP": "$시간"}, 5]},   # 예: "0:39"
                    {"$concat": ["0", "$시간"]},            # → "00:39"
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
    ]


def add_ts_range(pipeline, date_from, date_to):
    """YYYY.MM.DD 문자열의 기간 필터를 파이프라인에 추가"""
    ts_range = {}
    if date_from:
        ts_range["$gte"] = {"$dateFromString": {
            "dateString": date_from + " 00:00",
            "format": "%Y.%m.%d %H:%M"
        }}
    if date_to:
        ts_range["$lte"] = {"$dateFromString": {
            "dateString": date_to + " 23:59",
            "format": "%Y.%m.%d %H:%M"
        }}
    if ts_range:
        pipeline.append({"$match": {"ts": ts_range}})
    return pipeline


# ===== 라우트 =====
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


@app.get("/search")
def search_by_unique_id():
    """
    프론트 탭1: 고유번호 검색
    쿼리:
      - unique_id (또는 id) : 필수
      - date_from / date_to : YYYY.MM.DD (선택)
      - page / limit        : 선택 (기본 page=1, limit=200, 최대 1000)
    응답: [ { unique_id, 날짜, 시간, 인증샷 시리얼넘버, ts? } ]  ← 배열
    """
    if client is None or collection is None:
        return jsonify({"ok": False, "error": "DB 연결 실패"}), 500

    unique_id = request.args.get("unique_id") or request.args.get("id")
    if not unique_id:
        return dumps_json([])

    date_from = request.args.get("date_from")
    date_to = request.args.get("date_to")

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

    # 파이프라인 구성: unique_id → ts 생성 → (기간 필터) → 정렬 → 페이지 → 프로젝션
    pipeline = [
        {"$match": {"unique_id": unique_id}},
        *build_ts_pipeline(),
    ]
    pipeline = add_ts_range(pipeline, date_from, date_to)
    pipeline += [
        {"$sort": {"ts": 1, "_id": 1}},
        {"$skip": skip},
        {"$limit": limit},
        {"$project": {
            "_id": 0,
            "unique_id": 1,
            "날짜": 1,
            "시간": 1,
            "인증샷 시리얼넘버": 1,
            # 필요하면 ts를 내려도 됨: "ts": 1
        }}
    ]

    docs = list(collection.aggregate(pipeline))
    return dumps_json(docs)


@app.get("/search-serial")
def search_by_serial():
    """
    프론트 탭2: 시리얼 검색
    쿼리:
      - serial             : 필수 (정확 일치)
      - date_from/date_to  : 선택 (YYYY.MM.DD)
    응답: [ { unique_id, 날짜, 시간, 인증샷 시리얼넘버 } ]  ← 배열
    """
    if client is None or collection is None:
        return jsonify({"ok": False, "error": "DB 연결 실패"}), 500

    serial = request.args.get("serial")
    if not serial:
        return dumps_json([])

    date_from = request.args.get("date_from")
    date_to = request.args.get("date_to")

    # 기간이 없으면 간단 find (인덱스 타서 가장 빠름)
    projection = {
        "_id": 0,
        "unique_id": 1,
        "날짜": 1,
        "시간": 1,
        "인증샷 시리얼넘버": 1
    }
    if not date_from and not date_to:
        docs = list(
            collection.find({"인증샷 시리얼넘버": serial}, projection)
                      .sort([("unique_id", 1), ("날짜", 1), ("시간", 1)])
        )
        return dumps_json(docs)

    # 기간이 있으면 ts 생성 후 필터
    pipeline = [
        {"$match": {"인증샷 시리얼넘버": serial}},
        *build_ts_pipeline(),
    ]
    pipeline = add_ts_range(pipeline, date_from, date_to)
    pipeline += [
        {"$sort": {"unique_id": 1, "ts": 1}},
        {"$project": projection}
    ]
    docs = list(collection.aggregate(pipeline))
    return dumps_json(docs)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))
