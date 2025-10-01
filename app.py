# app.py
from flask import Flask, jsonify, request, Response
from flask_cors import CORS
from pymongo import MongoClient
from pymongo.collation import Collation
from bson import json_util
import os

app = Flask(__name__)
# 필요 도메인만 열면 더 안전합니다: CORS(app, resources={r"/search*": {"origins": ["https://hxlevent.github.io"]}})
CORS(app)

CONNECTION_STRING = os.environ.get("MONGO_URI")
client = None
collection = None

if CONNECTION_STRING:
    try:
        client = MongoClient(CONNECTION_STRING, serverSelectionTimeoutMS=5000)
        db = client['my_database']
        collection = db['my_collection']
        client.admin.command('ping')
        print("MongoDB 연결 성공!")
    except Exception as e:
        print(f"MongoDB 연결 실패: {e}")
        client = None
        collection = None

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
def search_records():
    # 연결 확인
    if client is None or collection is None:
        return jsonify({"ok": False, "error": "DB 연결 실패"}), 500

    # ★ 프론트 호환: id, unique_id 둘 다 허용
    search_id = request.args.get('unique_id') or request.args.get('id')
    if not search_id:
        # 프론트가 배열을 기대할 수도 있어 아래 형태를 유지
        return Response(json_util.dumps([]), mimetype="application/json")

    # (선택) 페이지네이션 파라미터—없으면 기본값
    try:
        limit = min(max(int(request.args.get("limit", 200)), 1), 1000)
    except Exception:
        limit = 200

    # (선택) 기간 필터 훅: YYYY.MM.DD
    date_from = request.args.get("date_from")
    date_to   = request.args.get("date_to")

    # 기본 쿼리
    query = {"unique_id": search_id}

    # 한글 필드명 그대로 쓰는 스키마 대응
    projection = {
        "_id": 0,                    # ★ ObjectId 직렬화 문제 회피
        "unique_id": 1,
        "날짜": 1,
        "시간": 1,
        "인증샷 시리얼넘버": 1
    }

    # 콜레이션 주의:
    # - simple locale 은 바이너리 비교(대소문자 구분). strength=2는 simple에선 의미 없음.
    # - 진짜 대소문자 무시 원하면 'en' 같은 locale + strength=2로 인덱스를 맞춰야 인덱스 탐.
    # 여기서는 정확 일치라고 하셨으니 collation 제거(= 인덱스 잘 탐).
    cursor = collection.find(query, projection).limit(limit)

    # 기간 필터를 서버에서 하고 싶으면 aggregate로 날짜/시간을 합쳐 ts 생성해 match 하세요.
    # (지금은 최소 수정판이라 find로만 처리)

    docs = list(cursor)

    # ★ flask.jsonify 대신 bson.json_util.dumps로 안전 직렬화
    return Response(
        json_util.dumps(docs, ensure_ascii=False),
        mimetype="application/json"
    )

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))
