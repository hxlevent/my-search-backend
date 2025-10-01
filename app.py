# app.py
from flask import Flask, jsonify, request
from flask_cors import CORS
from pymongo import MongoClient
import os

app = Flask(__name__)
CORS(app)

# MongoDB 연결 (보안을 위해 환경 변수 사용)
CONNECTION_STRING = os.environ.get("MONGO_URI")

client = None
collection = None

if CONNECTION_STRING:
    try:
        client = MongoClient(CONNECTION_STRING)
        db = client['my_database']
        collection = db['my_collection']
        # 데이터베이스의 'unique_id' 필드에 대한 대소문자 구분 없는 인덱스를 사용하도록 collation 설정
        # 2.5부에서 만든 인덱스를 직접 사용하라는 명령어입니다.
        case_insensitive_collation = collection.with_options(
            collation={'locale': 'simple', 'strength': 2}
        )
        client.admin.command('ping')
        print("MongoDB 연결 성공!")
    except Exception as e:
        print(f"MongoDB 연결 실패: {e}")
        client = None
else:
    print("MONGO_URI 환경 변수가 설정되지 않았습니다.")

@app.route('/')
def home():
    return "API 서버가 정상 작동 중입니다." if client else "DB 연결 실패"

@app.route('/search')
def search_records():
    if not client or not collection:
        return jsonify({"error": "DB 연결 실패"}), 500

    search_id = request.args.get('id', '')
    if not search_id:
        return jsonify([])

    # ★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★
    # 가장 빠르고 효율적인 최종 검색 방식입니다.
    # case_insensitive_collation을 사용하여 인덱스를 100% 활용합니다.
    results = list(case_insensitive_collation.find(
        {'unique_id': search_id}, 
        {'_id': 0}
    ))
    # ★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★

    return jsonify(results)
