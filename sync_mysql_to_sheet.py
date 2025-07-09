## SQL --> sheet

import pymysql
import gspread
import pandas as pd
from google.oauth2.service_account import Credentials
from datetime import datetime

# MySQL 연결 설정
conn = pymysql.connect(
    host='your_ip',
    user='your_user',
    password='your_password',
    db='db',
    charset='utf8mb4',
    cursorclass=pymysql.cursors.DictCursor
)

# 구글 시트 설정
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
creds = Credentials.from_service_account_file('service_account.json', scopes=scope)
gc = gspread.authorize(creds)

# 문서 키
MAIN_SHEET_KEY = '1AKF6DY4JatQCQcbatcjPqEyez-yk17X9SwFgZHrBPao' #구글 시트키 - 메인
LOG_SHEET_KEY = '1IjvCaTHotpBGH-bzUMpv6FQYzQvuXz-aEnJy9e-4QLg' # 구글 시트키 - 로그
main_sheet = gc.open_by_key(MAIN_SHEET_KEY)
log_sheet = gc.open_by_key(LOG_SHEET_KEY)

# === 유틸 함수 ===

def clear_and_write_df(df, worksheet):
    if df.empty:
        print(f"[업데이트] '{worksheet.title}' 비어있음 (데이터 부분만 클리어)")
        # 데이터 영역(2행부터 아래)만 클리어 (가정: 최대 1000행 정도)
        worksheet.batch_clear([f"A2:Z1000"])
        return

    # 기존 첫 행(칼럼명)은 유지, 데이터는 2행부터 덮어쓰기
    # 2행부터 기존 데이터 삭제
    worksheet.batch_clear([f"A2:Z1000"])

    # 데이터 배열 생성 (칼럼명 제외, 2행부터)
    data = df.astype(str).values.tolist()

    # 2행부터 데이터 입력 (A2부터)
    worksheet.update(f"A2", data)

    print(f"[업데이트] '{worksheet.title}'에 {len(df)}개 행 저장 완료 (칼럼명 유지)")


# === 내보내기 함수 ===

def export_auth():
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM auth")
        df = pd.DataFrame(cur.fetchall())
    clear_and_write_df(df, main_sheet.worksheet('인증'))


def export_total_log_view():
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM Total_log ORDER BY timestamp DESC LIMIT 2000")
        df = pd.DataFrame(cur.fetchall())
    try:
        ws = log_sheet.worksheet('Total_logView')
    except gspread.exceptions.WorksheetNotFound:
        ws = log_sheet.add_worksheet(title='Total_logView', rows="2000", cols="30")
    clear_and_write_df(df, ws)


def export_user_logs_separately():
    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT name FROM Total_log WHERE name IS NOT NULL AND name != ''")
        user_names = [row['name'] for row in cur.fetchall()]

    for name in user_names:
        sheet_name = f"{name}_log"[:100]
        with conn.cursor() as cur:
            cur.execute("""
                SELECT * FROM Total_log 
                WHERE name = %s 
                ORDER BY timestamp DESC 
                LIMIT 300
            """, (name,))
            user_df = pd.DataFrame(cur.fetchall())

        if user_df.empty:
            continue

        try:
            user_ws = log_sheet.worksheet(sheet_name)
        except gspread.exceptions.WorksheetNotFound:
            user_ws = log_sheet.add_worksheet(title=sheet_name, rows="1000", cols="30")
        clear_and_write_df(user_df, user_ws)


# === 실행 함수 ===

def run_with_user_logs():
    export_auth()
    export_total_log_view()
    export_user_logs_separately()
    print("✅ 인증 + 최근 로그 + 유저별 로그 시트 내보내기 완료")


# === 진입점 ===
if __name__ == '__main__':
    run_with_user_logs()
    conn.close()
