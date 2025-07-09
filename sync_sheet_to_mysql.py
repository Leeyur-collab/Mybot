import pymysql
import gspread
import pandas as pd
import datetime
from google.oauth2.service_account import Credentials

# DB연결
def get_conn():
    conn = pymysql.connect(
        host='your_ip_address',  # GCP나 AWS IP 주소
        user='your_user',  # 실제 유저 이름으로 변경
        password='your_password',  # 실제 패스워드로 변경
        charset='utf8mb4', 
        cursorclass=pymysql.cursors.DictCursor
    )
    return conn
  
# DB가 없으면 생성
def create_database_if_not_exists(conn):
    with conn.cursor() as cur:
        cur.execute("CREATE DATABASE IF NOT EXISTS bot;")
    conn.commit()

# 'bot' DB에 연결
def get_bot_db_conn():
    return pymysql.connect(
        host='your_ip_address',  # GCP나 AWS IP 주소
        user='your_user',  # 실제 유저 이름으로 변경
        password='your_password',  # 실제 패스워드로 변경
        db='bot',
        charset='utf8mb4', 
        cursorclass=pymysql.cursors.DictCursor
    )

# 테이블 생성
def ensure_table_exists(cur, table_name, create_sql):
    cur.execute(f"SHOW TABLES LIKE '{table_name}'")
    result = cur.fetchone()
    if not result:
        print(f"테이블 {table_name} 없음. 생성 중...")
        cur.execute(create_sql)
        print(f"✅ 테이블 {table_name} 생성 완료.")

# 토탈 로그 테이블 생성 SQL
def create_total_log_table():
    return """
    CREATE TABLE IF NOT EXISTS `Total_log` (
        timestamp DATETIME,
        user_id VARCHAR(100),
        id_code VARCHAR(64),
        name VARCHAR(64),
        input TEXT,
        type VARCHAR(64),
        select_path TEXT,
        bot_response TEXT,
        PRIMARY KEY (timestamp, name)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """

# 타임스탬프 찍기
def safe_datetime(val):
    if val is None:
        return None
    if isinstance(val, str):
        val = val.strip()
        if val in ['', 'None', 'NaT']:
            return None
        try:
            # 문자열이면 datetime으로 변환 시도
            return pd.to_datetime(val)
        except Exception:
            return None
    if isinstance(val, pd.Timestamp):
        if pd.isna(val):
            return None
        return val.to_pydatetime()
    if isinstance(val, datetime.datetime):
        return val
    # 그 외 타입이면 None 처리
    return None

# 정수 변환
def safe_int(val, default=0):
    if val in [None, '', 'None']:
        return default
    try:
        return int(val)
    except (ValueError, TypeError):
        return default

# 실수 변환 
def safe_float(val, default=None):
    if val in [None, '', 'None']:
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default

# 구글 시트 접속 & 시트값 가져오기
def get_ws(sheet_key, sheet_name):
    creds = Credentials.from_service_account_file('service_account.json', scopes=[
        'https://spreadsheets.google.com/feeds',
        'https://www.googleapis.com/auth/drive'
    ])
    gc = gspread.authorize(creds)
    sheet = gc.open_by_key(sheet_key)
    return sheet.worksheet(sheet_name)


# 토탈로그 확인 후 동기화
def ensure_total_log_table_exists(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS Total_log (
            timestamp DATETIME,
            user_id VARCHAR(100),
            id_code VARCHAR(64),
            name VARCHAR(64),
            input TEXT,
            type VARCHAR(64),
            select_path TEXT,
            bot_response TEXT,
            PRIMARY KEY (timestamp, name)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """)

def ensure_auth_table_exists(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS auth (
            id_code VARCHAR(64) PRIMARY KEY,
            name VARCHAR(64),
            userId VARCHAR(100),
            job VARCHAR(64),
            height FLOAT,
            attention INT,
            power INT,
            obs INT,
            luck INT,
            wilpower INT,
            coin INT,
            gain_path TEXT,
            auth_time DATETIME
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """)

def sync_auth(conn):
    df = pd.DataFrame(get_ws('1gF10CYj794dZtHdepRz-78VgpUEWlweKX6bEfA3Fa8w', '인증').get_all_records())
    df = df.where(pd.notnull(df), None)
    with conn.cursor() as cur:
        cur.execute("DELETE FROM auth")
        for _, row in df.iterrows():
            cur.execute("""
                INSERT INTO auth (
                    id_code, name, userId, job, height,
                    attention, power, obs, luck, wilpower,
                    coin, gain_path, auth_time
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    name=VALUES(name),
                    userId=VALUES(userId),
                    job=VALUES(job),
                    height=VALUES(height),
                    attention=VALUES(attention),
                    power=VALUES(power),
                    obs=VALUES(obs),
                    luck=VALUES(luck),
                    wilpower=VALUES(wilpower),
                    coin=VALUES(coin),
                    gain_path=VALUES(gain_path),
                    auth_time=VALUES(auth_time)
            """, (
                row['id_code'],
                row['Name'],
                row['userId'],
                row['직업'],
                safe_float(row['키']),
                safe_int(row['주목도']),
                safe_int(row['힘']),
                safe_int(row['관찰']),
                safe_int(row['행운']),
                safe_int(row['정신력']),
                safe_int(row['소지금'], default=None),
                row['획득 경로'] if row['획득 경로'] not in [None, '', 'None'] else None,
                safe_datetime(row['인증시각'])
            ))
    conn.commit()
    print("✅ 인증 시트 → 'auth' 테이블 동기화 완료")

def ensure_josa_table_exists(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS 조사 (
            선택경로 TEXT,
            장소1 TEXT,
            장소2 TEXT,
            장소3 TEXT,
            장소4 TEXT,
            장소5 TEXT,
            타겟 TEXT,
            조건 TEXT,
            조건2 TEXT,
            조건3 TEXT,
            출력지문 TEXT,
            선택지 TEXT
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """)

def sync_josa(conn):
    df = pd.DataFrame(get_ws('1gF10CYj794dZtHdepRz-78VgpUEWlweKX6bEfA3Fa8w', '조사').get_all_records())
    df = df.where(pd.notnull(df), None)
    df = df.replace('', None)
    with conn.cursor() as cur:
        # 1. 테이블 전체 초기화
        cur.execute("DELETE FROM 조사")
        # 2. 시트 내용을 다시 삽입
        for row in df.to_dict(orient='records'):
            cur.execute("""
                INSERT INTO 조사 (선택경로, 장소1, 장소2, 장소3, 장소4, 장소5, 타겟,
                조건, 조건2, 조건3, 출력지문, 선택지)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                row.get('선택경로'), row.get('장소1'), row.get('장소2'), row.get('장소3'), row.get('장소4'),
                row.get('장소5'), row.get('타겟'), row.get('조건'), row.get('조건2'),
                row.get('조건3'), row.get('출력지문'), row.get('선택지')
            ))
    conn.commit()
    print("✅ 조사(josa) 테이블 초기화 후 동기화 완료")

def ensure_random_table_exists(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS 랜덤 (
            `SELECT 답변 리스트` TEXT,
            `랜덤 키워드` VARCHAR(255)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """)

def sync_random(conn):
    df = pd.DataFrame(get_ws('1AKF6DY4JatQCQcbatcjPqEyez-yk17X9SwFgZHrBPao', '랜덤').get_all_records())
    df = df.where(pd.notnull(df), None).replace('', None)

    with conn.cursor() as cur:
        ensure_random_table_exists(cur)
        cur.execute("DELETE FROM 랜덤")
        for row in df.to_dict(orient='records'):
            cur.execute("""
                INSERT INTO 랜덤 (`SELECT 답변 리스트`, `랜덤 키워드`)
                VALUES (%s, %s)
            """, (
                row.get('SELECT 답변 리스트'),
                row.get('랜덤 키워드')
            ))
    conn.commit()
    print("✅ 랜덤 시트 → '랜덤' 테이블 동기화 완료")


def run():
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            ensure_total_log_table_exists(cur)
            ensure_random_table_exists(cur)
        sync_auth(conn)
        sync_josa(conn)
        sync_random(conn)
        print("🎉 인증, 조사, 랜덤 시트 동기화 완료 및 테이블 확인 완료")
    finally:
        conn.close()

if __name__ == '__main__':
    run()
