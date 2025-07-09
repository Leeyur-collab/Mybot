import re
from datetime import datetime
from utils import get_conn
import pymysql

def extract_coin_from_text(text):
    patterns = [
        r"코인(?:을)?\s*(\d+)\s*개\s*(?:획득|습득|받|얻)",         # 코인을 10개 습득
        r"(\d+)\s*개\s*코인(?:을)?\s*(?:획득|습득|받|얻)",         # 10개 코인을 습득
        r"(\d+)\s*코인\s*(?:획득|습득|받|얻)",                     # 10코인 획득
        r"코인\s*(\d+)\s*개",                                     # 코인 10개
        r"(\d+)\s*코인",                                          # 10코인
    ]
    
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            return int(match.group(1))
    
    return 0  # 매칭되지 않으면 0


def calculate_auto_settlement(user_id, name, id_code):
    try:
        conn = get_conn()
        with conn.cursor(pymysql.cursors.DictCursor) as cur :
            # 1) 현재 소지금 조회
            cur.execute("SELECT coin FROM auth WHERE userId = %s", (user_id,))
            row = cur.fetchone()
            if not row:
                return "인증된 사용자를 찾을 수 없습니다."

            current_coin = int(row.get("coin", 0))

            # 2) user_log 테이블에서 마지막 정산 이후 조사 로그 추출
            cur.execute("""
                SELECT timestamp, type, bot_response
                FROM user_log
                WHERE user_id = %s
                ORDER BY timestamp DESC
            """, (user_id,))
            logs = cur.fetchall()

            total_new_coin = 0
            for log in logs:
                if log["type"] == "settle_tree":
                    break  # 마지막 정산 위치 기준으로 종료
                if log["type"] == "investigate_tree":
                    coin = extract_coin_from_text(log["bot_response"])
                    total_new_coin += coin

            # 3) 새로운 소지금 업데이트
            new_coin = current_coin + total_new_coin
            cur.execute("UPDATE auth SET coin = %s WHERE userId = %s", (new_coin, user_id))
            conn.commit()

            return f"금일 일반 조사를 통해 {total_new_coin} 코인을 획득하였습니다.\n{name}님 현재 소지 코인은 {new_coin}개 입니다."

    except Exception as e:
        print(f"자동 정산 오류: {e}")
        return "정산 중 오류가 발생했습니다."
    finally:
        conn.close()


def check_coin_balance(user_id):
    try:
        conn = get_conn()
        with conn.cursor(pymysql.cursors.DictCursor) as cur:
            cur.execute("SELECT name, coin FROM auth WHERE userId = %s", (user_id,))
            row = cur.fetchone()
            if not row:
                return "인증된 사용자를 찾을 수 없습니다."

            name = row.get("name", "사용자")
            coin = row.get("coin", 0)
            return f"{name}님의 현재 소지 코인은 {coin}개 입니다."

    except Exception as e:
        print(f"소지금 확인 오류: {e}")
        return "소지금 확인 중 오류가 발생했습니다."
    finally:
        conn.close()
