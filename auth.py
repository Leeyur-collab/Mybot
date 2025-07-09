import pymysql
from utils import create_response, get_conn
from datetime import datetime

def find_auth_by_field(field_name, value):
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            sql = f"SELECT * FROM auth WHERE {field_name} = %s"
            cur.execute(sql, (value,))
            row = cur.fetchone()
            return row
    except Exception as e:
        print(f"인증 조회 실패 ({field_name}): {e}")
    finally:
        conn.close()
    return None

def find_auth_by_id_code(id_code):
    return find_auth_by_field('id_code', id_code)

def find_auth_by_user_id(user_id):
    return find_auth_by_field('userId', user_id)

def require_auth(user_id):
    auth_row = find_auth_by_field('userId', user_id)
    if not auth_row:
        return create_response("먼저 인증코드를 입력해 주세요.")
    return create_response("이미 인증된 사용자입니다.\\n무엇을 진행하시겠습니까?\\n\\n▶ 조사")

def update_user_auth(user_id, id_code):
    try:
        conn = get_conn()
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE auth SET userId = %s, auth_time = %s WHERE id_code = %s
            """, (user_id, now, id_code))
        conn.commit()
        print(f"[DEBUG] 인증 정보 업데이트 완료: id_code={id_code}, userId={user_id}")
    except Exception as e:
        print(f"인증 정보 업데이트 실패: {e}")
    finally:
        conn.close()
