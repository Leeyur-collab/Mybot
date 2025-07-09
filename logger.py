import pymysql
import gspread
from utils import get_conn
from flask import jsonify
from datetime import datetime

def get_user_status(user_id):
    conn = get_conn()
    try:
        with conn.cursor(pymysql.cursors.DictCursor) as cur:
            cur.execute("SELECT type, select_path FROM Total_log  WHERE user_id = %s", (user_id,))
            status = cur.fetchone()
        if status is None:
            return {'type': 'auth', 'select_path': ''}
        return status
    finally:
        conn.close()
        
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

            # user_log 테이블 입력 (제거)
            # sql_user = """
            #     INSERT INTO user_log (timestamp, user_id, id_code, name, input, type, select_path, bot_response)
            #     VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            # """
            # cur.execute(sql_user, (now, user_id, id_code, name, user_input, type_, select_path, bot_response))

        conn.commit()

    except Exception as e:
        print(f"[log_all_mysql] 로깅 실패: {e}")

    finally:
        conn.close()
