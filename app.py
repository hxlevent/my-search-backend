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
case_insensitive_collation = None # '만능 검색기'를 모두가 알 수 있도록 여기에 선언

if CONNECTION_STRING:
    try:
        client = MongoClient(CONNECTION_STRING)
        db = client['my_database']
        # collection 변수를 먼저 정의합니다.
        collection = db['my_collection']

        # 이제 collection을 사용하여 '만능 검색기'를 만듭니다.
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
    # case_insensitive_collation이 준비되었는지도 함께 확인
    if not client or not collection or not case_insensitive_collation:
        return jsonify({"error": "DB 연결 실패"}), 500

    search_id = request.args.get('id', '')
    if not search_id:
        return jsonify([])

    # ★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★
    # 이제 '만능 검색기'를 확실히 찾아서 사용할 수 있습니다.
    results = list(case_insensitive_collation.find(
        {'unique_id': search_id}, 
        {'_id': 0}
    ))
    # ★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★

    return jsonify(results)
