# app.py
from flask import Flask, jsonify, request
from flask_cors import CORS
from pymongo import MongoClient
import os

app = Flask(__name__)
CORS(app)

# MongoDB 연결 (보안을 위해 환경 변수 사용)
CONNECTION_STRING = os.environ.get("MONGO_URI", "YOUR_FALLBACK_MONGO_URI") # Render에서 설정할 예정

client = None
collection = None

if CONNECTION_STRING != "YOUR_FALLBACK_MONGO_URI":
    try:
        client = MongoClient(CONNECTION_STRING)
        db = client['my_database']
        collection = db['my_collection']
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
    if not collection: return jsonify({"error": "DB 연결 실패"}), 500
    search_id = request.args.get('id', '')
    if not search_id: return jsonify([])
    results = list(collection.find({'unique_id': {'$regex': f'^{search_id}$', '$options': 'i'}}, {'_id': 0}))
    return jsonify(results)
