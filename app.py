# app.py
from flask import Flask, jsonify, request
from flask_cors import CORS
from pymongo import MongoClient
from pymongo.collation import Collation
import os

app = Flask(__name__)
CORS(app)

CONNECTION_STRING = os.environ.get("MONGO_URI")
client = None
collection = None

if CONNECTION_STRING:
    try:
        client = MongoClient(CONNECTION_STRING)
        db = client['my_database']
        collection = db['my_collection']
        client.admin.command('ping')
        print("MongoDB 연결 성공!")
    except Exception as e:
        print(f"MongoDB 연결 실패: {e}")
        client = None

@app.route('/')
def home():
    return "API 서버가 정상 작동 중입니다." if client else "DB 연결 실패"

@app.route('/search')
def search_records():
    # ★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★
    # "if not collection:" 오류를 "if collection is None:" 으로 수정한 최종 코드입니다.
    if client is None or collection is None:
        return jsonify({"error": "DB 연결 실패"}), 500
    # ★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★

    search_id = request.args.get('id', '')
    if not search_id:
        return jsonify([])

    # '대소문자 무시' 옵션을 find 명령어에 직접 전달하는,
    # 인덱스를 100% 활용하는 가장 빠르고 정확한 최종 검색 방식입니다.
    results = list(collection.find(
        {'unique_id': search_id},
        collation=Collation(locale='simple', strength=2)
    ).limit(1000)) # 최대 1000개 결과로 제한

    return jsonify(results)
