from utils2 import extract_bracket_content, fill_random_in_text, get_survey_type_by_day, create_response, get_conn, get_user_status
from auth import find_auth_by_user_id
import pymysql.cursors
import re

# SQL문 정의
SQL_SELECT_ALL_JOSA = """
    SELECT 선택경로, 장소1, 장소2, 장소3, 장소4, 장소5,
           타겟,
           NULLIF(조건, '') AS 조건,
           NULLIF(조건2, '') AS 조건2,
           NULLIF(조건3, '') AS 조건3,
           출력지문, 선택지
    FROM 조사
"""

SQL_SELECT_JOSA_BY_PATH_IS_NULL = "SELECT * FROM 조사 WHERE 선택경로 IS NULL OR 선택경로 = ''"
SQL_SELECT_JOSA_BY_PATH = "SELECT * FROM 조사 WHERE 선택경로 = %s"
SQL_UPDATE_EARNED_PATHS = "UPDATE auth SET gain_path = %s WHERE id_code = %s"

def normalize_path(path):
    if not path:
        return ''
    return re.sub(r'\s+', ' ', path.strip())

def normalize_value(value):
    if value is None:
        return None
    if isinstance(value, str):
        val = value.strip()
        if val == '' or val.lower() == 'null' or val == '#ref!':
            return None
        return val
    return value

# 선택경로 기준 DB 조회 함수
def get_josa_rows_by_select_path(path):
    conn = None
    try:
        conn = get_conn()
        with conn.cursor(pymysql.cursors.DictCursor) as cur:
            if path is None or path.strip() == '':
                sql = SQL_SELECT_JOSA_BY_PATH_IS_NULL
                print(f"[SQL] {sql}")
                cur.execute(sql)
            else:
                sql = SQL_SELECT_JOSA_BY_PATH
                print(f"[SQL] {sql} -- param: {path}")
                cur.execute(sql, (path,))
            rows = cur.fetchall()
            return rows
    except Exception as e:
        print(f"[ERROR] get_josa_rows_by_select_path 실패: {e}")
        return []
    finally:
        if conn:
            conn.close()

# 조사 데이터 전체 조회
def get_all_josa_records():
    conn = None
    try:
        conn = get_conn()
        with conn.cursor(pymysql.cursors.DictCursor) as cur:
            print(f"[SQL] {SQL_SELECT_ALL_JOSA.strip()}")
            cur.execute(SQL_SELECT_ALL_JOSA)
            rows = cur.fetchall()
            for row in rows:
                for key in row.keys():
                    row[key] = normalize_value(row[key])
            return rows
    except Exception as e:
        print(f"[ERROR] 조사 시트 전체 조회 실패: {e}")
        return []
    finally:
        if conn:
            conn.close()

# 코인 획득 경로 업데이트
def update_earned_paths(user_id_code, earned_paths, new_path):
    if new_path in earned_paths:
        return
    earned_paths.append(new_path)
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            updated_path_str = ",".join(earned_paths)
            sql = SQL_UPDATE_EARNED_PATHS
            print(f"[SQL] {sql} -- params: {updated_path_str}, {user_id_code}")
            cur.execute(sql, (updated_path_str, user_id_code))
        conn.commit()
        print(f"[INFO] 사용자 {user_id_code}의 획득 경로 업데이트: {updated_path_str}")
    except Exception as e:
        print(f"[ERROR] 획득 경로 업데이트 중 오류 발생: {e}")
    finally:
        if conn:
            conn.close()

# 조사 트리 로직
def investigate_tree_logic(select_path, user_input, user_auth_row=None, survey_type=None):
    try:
        print(f"[DEBUG] 조사 시작 - select_path: '{select_path}', user_input: '{user_input}'")

        records = get_all_josa_records()

        user_id_code = user_auth_row.get('id_code') if user_auth_row else None
        user_name = user_auth_row.get('name') if user_auth_row else None
        user_kakao_id = user_auth_row.get('userId', '') if user_auth_row else ''
        user_job = user_auth_row.get('job', '') if user_auth_row else ''
        gain_path = user_auth_row.get('gain_path') or '' if user_auth_row else ''
        earned_paths = [p.strip() for p in gain_path.split(',') if p.strip()]

        input_str = user_input if isinstance(user_input, str) else ''
        processed_input = extract_bracket_content(input_str) or input_str.strip()
        print(f"[DEBUG] 처리된 입력: '{processed_input}'")

        # 조사 시작 조건 (초기)
        if normalize_path(select_path) == '' and processed_input in ['', '조사']:
            print("[DEBUG] 초기 조사 시작 조건 충족")

            initial_rows = get_josa_rows_by_select_path(None)
            print(f"[DEBUG] 초기 조사 row 수: {len(initial_rows)}")

            for row in initial_rows:
                sp = normalize_value(row.get("선택경로"))
                c1 = normalize_value(row.get("조건"))
                c2 = normalize_value(row.get("조건2"))
                c3 = normalize_value(row.get("조건3"))

                if not sp and not c1 and not c2 and not c3:
                    output = fill_random_in_text(row.get("출력지문", "조사를 시작합니다.").strip())
                    options = [opt.strip() for opt in (row.get("선택지") or "").split(',') if opt.strip()]
                    options_text = '\n'.join(f"▶ {opt}" for opt in options)
                    print("[WARNING] 동일 초기 입력 반복 가능성 있음")
                    return f"{output}\n{options_text}".strip(), ""

            print("[DEBUG] 초기 조사 조건에 맞는 행 없음")
            return ("조건에 맞는 조사 항목이 없습니다.\n"
                    "'처음으로' 입력하거나 '이전으로' 입력해 주세요."), ""

        # 명령어 처리
        if processed_input == "처음으로":
            return investigate_tree_logic('', '', user_auth_row, survey_type)
        if processed_input == "이전으로":
            cleaned_path = normalize_path(select_path)
            path_parts = cleaned_path.split() if cleaned_path else []
            new_path = ' '.join(path_parts[:-1]) if len(path_parts) > 0 else ''
            new_path = normalize_path(new_path)
            print("[DEBUG] '이전으로' 처리됨")
            return investigate_tree_logic(new_path, '', user_auth_row, survey_type)
        if processed_input in ["조사종료", "그만", "메인으로", "종료"]:
            print("[DEBUG] 조사 종료 명령 처리됨")
            return "조사를 종료합니다.", select_path

        select_path = normalize_path(select_path)

        # 현재 경로 계산
        if select_path and processed_input == select_path.split(' ')[-1]:
            current_path = select_path
        else:
            current_path = f"{select_path} {processed_input}" if select_path else processed_input
        current_path = normalize_path(current_path)
        print(f"[DEBUG] current_path: '{current_path}'")

        matched_rows_acquire = []
        matched_rows_normal = []

        for row in records:
            row_path = normalize_path(row.get("선택경로") or '')
            if row_path != current_path:
                continue

            cond1 = normalize_value(row.get('조건'))
            cond2 = normalize_value(row.get('조건2'))
            cond3 = normalize_value(row.get('조건3'))

            cond1_ok = True
            if cond1:
                try:
                    cond1_ok = any(c in [user_name, str(user_id_code), user_kakao_id, user_job] for c in cond1.split())
                except Exception as e:
                    print(f"[ERROR] cond1 split error: {e} - cond1: {cond1}")
                    cond1_ok = False

            cond2_ok = True
            if cond2:
                try:
                    cond2_ok = all(c == survey_type for c in cond2.split())
                except Exception as e:
                    print(f"[ERROR] cond2 split error: {e} - cond2: {cond2}")
                    cond2_ok = False

            if cond1_ok and cond2_ok:
                if cond3 == "습득":
                    matched_rows_acquire.append(row)
                elif cond3 is None:
                    matched_rows_normal.append(row)

        print(f"[DEBUG] 습득 행 수: {len(matched_rows_acquire)}, 일반 행 수: {len(matched_rows_normal)}")

        if matched_rows_acquire:
            if current_path not in earned_paths:
                row = matched_rows_acquire[0]
                output = fill_random_in_text(row.get("출력지문", "").strip())
                options = [f"▶ {opt.strip()}" for opt in (row.get("선택지") or "").split(',') if opt.strip()]
                update_earned_paths(user_id_code, earned_paths, current_path)
                options_text = '\n'.join(options)
                print(f"[DEBUG] 신규 습득 경로 출력")
                return f"{output}\n{options_text}".strip(), current_path
            elif matched_rows_normal:
                row = matched_rows_normal[0]
                output = fill_random_in_text(row.get("출력지문", "").strip())
                options = [f"▶ {opt.strip()}" for opt in (row.get("선택지") or "").split(',') if opt.strip()]
                options_text = '\n'.join(options)
                print(f"[DEBUG] 이미 습득 경로에서 일반 출력")
                return f"{output}\n{options_text}".strip(), current_path

        if matched_rows_normal:
            row = matched_rows_normal[0]
            output = fill_random_in_text(row.get("출력지문", "").strip())
            options = [f"▶ {opt.strip()}" for opt in (row.get("선택지") or "").split(',') if opt.strip()]
            options_text = '\n'.join(options)
            print(f"[DEBUG] 조건3 없음 출력")
            return f"{output}\n{options_text}".strip(), current_path

        # 다음 선택지 추출
        next_options = set()
        for row in records:
            path = normalize_path(row.get("선택경로") or "")
            if path.startswith(current_path + ' '):
                remainder = path[len(current_path):].strip()
                if remainder:
                    next_step = remainder.split()[0]
                    next_options.add(next_step)
        if next_options:
            options_text = '\n'.join(f"▶ {opt}" for opt in sorted(next_options))
            print(f"[DEBUG] 다음 선택지: {options_text}")
            return options_text, current_path

        return ("조건에 맞는 조사 항목이 없습니다.\n"
                "'처음으로'를 입력하여 다시 시작하시거나, '이전으로'를 입력하여 이전 경로로 돌아가주세요."), current_path

    except Exception as e:
        print(f"[ERROR] 조사 트리 처리 오류: {e}")
        return ("조사 처리 중 오류가 발생했습니다. 다시 시도해 주세요.\n"
                "이 메세지가 반복될 시 운영진에 보고 부탁드립니다."), select_path

# 최초 조사 진입 함수
def skill_investigate_entry(user_id):
    auth_row = find_auth_by_user_id(user_id)
    if not auth_row:
        return create_response("먼저 인증코드를 입력해 주세요.")

    user_status = get_user_status(user_id) or {}
    select_path = user_status.get('select_path') or ''
    survey_type = user_status.get('type') or ''

    msg, new_path = investigate_tree_logic(select_path, '', auth_row, survey_type)
    return create_response(msg)
