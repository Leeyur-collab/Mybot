import pymysql  
from flask import jsonify
from datetime import datetime, timedelta
import pytz
import random
import re

KST = pytz.timezone('Asia/Seoul')

# DB 연결
def get_conn():
    return pymysql.connect(
        host='34.68.132.37', user='admin', password='ahrvysmswkehdghk', db='bot',
        charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor
    )

# 대답 생성
def create_response(msg):
    return jsonify({
        "version": "2.0",
        "template": {"outputs": [{"simpleText": {"text": msg}}]}
    })

# 로그 저장 (토탈)
def log_all(user_id, id_code, name, user_input, type_, select_path, bot_response):
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    try:
        conn = get_conn()
        with conn.cursor() as cur:
            # Total_log 테이블 입력
            sql_total = """
                INSERT INTO Total_log (timestamp, user_id, id_code, name, input, type, select_path, bot_response)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            cur.execute(sql_total, (now, user_id, id_code, name, user_input, type_, select_path, bot_response))

        conn.commit()

    finally:
        conn.close()

# 사용자 정보 및 위치 조회
def get_user_status(user_id):
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT type, select_path 
                FROM Total_log 
                WHERE user_id = %s
                ORDER BY timestamp DESC
                LIMIT 1
            """, (user_id,))
            row = cur.fetchone()
            if row:
                return {'type': row['type'], 'select_path': row['select_path']}
    except Exception as e:
        print(f"[ERROR] 사용자 상태 조회 오류: {e}")
    finally:
        conn.close()
    return {'type': '', 'select_path': ''}

# 오후 9시 ~ 10시 체크
def is_operating_hour():
    now = datetime.now(KST)
    return 21 <= now.hour <= 22

# 일상조사 비일상조사 파싱
def get_survey_type_by_day():
    now = datetime.now(KST)
    weekday = now.weekday()
    if weekday in [4, 5]:
        return '비일상조사'
    else:
        return '일상조사'

# [] 안의 내용만 추출
def extract_bracket_content(text):
    match = re.search(r'\[(.*?)\]', text)
    return match.group(1).strip() if match else ''

# n시간 만에 접속 체크 
def is_long_time_no_see(last_time, hours=18):
    try:
        last_dt = datetime.strptime(last_time, '%Y-%m-%d %H:%M:%S')
        return (datetime.now() - last_dt) > timedelta(hours=hours)
    except Exception:
        return False

# 랜덤 키워드 추출
def get_random_answer(keyword):
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT `답변 리스트` FROM `random` WHERE `랜덤 키워드` = %s LIMIT 1
            """, (keyword.strip(),))
            row = cur.fetchone()
            if row and row.get('답변 리스트'):
                options = [opt.strip() for opt in row['답변 리스트'].split(',') if opt.strip()]
                if options:
                    return random.choice(options)
    except Exception as e:
        print(f"[ERROR] get_random_answer 오류: {e}")
    finally:
        conn.close()
    return '랜덤 정보가 없습니다'

# 랜덤 답변 채우기
def fill_random_in_text(text):
    def replacer(match):
        keyword = match.group(1) or match.group(2)
        return get_random_answer(keyword)
    return re.sub(r'\{랜덤(?::\s*([^\}]+))?\}', replacer, text)
