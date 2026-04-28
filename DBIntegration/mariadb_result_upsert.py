import os
import sys
import json
import csv
import glob
import logging
import time
import argparse
from pathlib import Path
import pymysql

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("DBIntegration")

BASE_DIR = Path(__file__).resolve().parent

def load_db_config():
    CONFIG_PATH = BASE_DIR / "db_config.json"
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(f"DB 설정 파일이 없습니다: {CONFIG_PATH}")
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

def get_db_connection(config):
    return pymysql.connect(
        host=config['host'],
        port=config['port'],
        user=config['user'],
        password=config['password'],
        database=config['database'],
        charset=config.get('charset', 'utf8mb4'),
        cursorclass=pymysql.cursors.DictCursor
    )

def generate_upsert_query(table_name, columns):
    """CSV의 컬럼들을 바탕으로 동적 INSERT ... ON DUPLICATE KEY UPDATE 쿼리를 생성합니다."""
    cols_str = ", ".join(f"`{c}`" for c in columns)
    vals_str = ", ".join(["%s"] * len(columns))
    
    # ON DUPLICATE KEY UPDATE 절 생성
    update_str = ", ".join(f"`{c}`=VALUES(`{c}`)" for c in columns)
    
    query = f"INSERT INTO `{table_name}` ({cols_str}) VALUES ({vals_str}) ON DUPLICATE KEY UPDATE {update_str}"
    return query

def clean_old_races(conn, date_str, venue):
    """tmp_races 테이블에서 해당 날짜와 경기장의 데이터를 삭제(초기화)합니다."""
    formatted_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
    query = "DELETE FROM tmp_races WHERE RCDATE = %s AND MEET = %s"
    
    try:
        conn.ping(reconnect=True)
        with conn.cursor() as cursor:
            cursor.execute(query, (formatted_date, venue))
            deleted_rows = cursor.rowcount
        conn.commit()
        logger.info(f"🧹 초기화 완료: tmp_races 에서 {formatted_date} {venue} 경기 데이터 {deleted_rows}건 삭제 (취소마 방어)")
        return True
    except Exception as e:
        conn.rollback()
        logger.error(f"❌ 초기화(DELETE) 실패: {e}")
        return False

def process_csv_file(conn, csv_path, table_name):
    logger.info(f"파일 처리 중: {csv_path.name} -> 테이블: {table_name}")
    
    with open(csv_path, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        if not fieldnames:
            logger.warning(f"빈 CSV 파일입니다: {csv_path.name}")
            return False
            
        query = generate_upsert_query(table_name, fieldnames)
        
        # 데이터를 리스트 튜플 형태로 변환
        data_to_insert = []
        for row in reader:
            # 빈 문자열은 None(DB의 NULL)으로 변환
            row_data = tuple(row[col] if row[col] != "" else None for col in fieldnames)
            data_to_insert.append(row_data)
            
        if not data_to_insert:
            logger.info("삽입할 데이터가 없습니다.")
            return True

        max_retries = 3
        for attempt in range(1, max_retries + 1):
            try:
                # 연결이 끊겼을 수 있으므로 핑 체크 및 자동 재연결
                conn.ping(reconnect=True)
                with conn.cursor() as cursor:
                    # executemany로 대량 데이터 한 번에 삽입 (매우 빠름)
                    cursor.executemany(query, data_to_insert)
                conn.commit()
                logger.info(f"성공: {len(data_to_insert)} 건의 데이터가 DB에 반영되었습니다 (Upsert).")
                return True
            except pymysql.MySQLError as e:
                conn.rollback()
                error_code = e.args[0]
                # 1213: Deadlock, 1205: Lock wait timeout, 2006: Server gone away, 2013: Connection lost
                if error_code in (1213, 1205, 2006, 2013) and attempt < max_retries:
                    wait_time = 2 ** attempt
                    logger.warning(f"DB 일시적 오류 발생(코드:{error_code}). {wait_time}초 후 재시도 합니다... ({attempt}/{max_retries})")
                    time.sleep(wait_time)
                else:
                    logger.error(f"DB 삽입 중 치명적 오류 발생: {e}")
                    return False
            except Exception as e:
                conn.rollback()
                logger.error(f"예상치 못한 오류 발생: {e}")
                return False
        return False

def upload_result_csv_to_db(target_date, target_venue):
    config = load_db_config()
    
    # 아이디/비밀번호가 템플릿 그대로면 경고 후 종료
    if config['user'] == "YOUR_DB_ID" or config['password'] == "YOUR_DB_PASSWORD":
        logger.error("DB 설정(db_config.json)의 아이디와 비밀번호를 올바르게 입력해주세요!")
        return
        
    try:
        conn = get_db_connection(config)
    except Exception as e:
        logger.error(f"DB 연결 실패: {e}")
        return

    # [핵심] 기존 계획 데이터 덮어쓰기를 위한 DELETE 수행
    if not clean_old_races(conn, target_date, target_venue):
        logger.error("기존 데이터 초기화에 실패하여 프로세스를 중단합니다.")
        conn.close()
        return

    # 업로드 대상 정의 (Mode 1 과거 결과용)
    TARGETS = [
        (BASE_DIR.parent / "WebCrawler" / "data" / f"race_planning_{target_venue}_{target_date}.csv", "tmp_races"),
        (BASE_DIR.parent / "TRNOCrwaler" / "data" / f"TRNO_result_{target_venue}_{target_date}.csv", "tmp_trainers"),
        (BASE_DIR.parent / "HRNOCrawler" / "data" / f"HRNO_result_{target_venue}_{target_date}.csv", "tmp_horses"),
        (BASE_DIR.parent / "JKNOCrawler" / "data" / f"JKNO_result_{target_venue}_{target_date}.csv", "tmp_jockeys")
    ]
    
    total_files = 0
    success_count = 0
    
    for pattern_path, table_name in TARGETS:
        csv_files = glob.glob(str(pattern_path))
        if not csv_files:
            logger.info(f"스킵: 업로드할 CSV 파일 없음 ({pattern_path.name})")
            continue
            
        for file_path in csv_files:
            total_files += 1
            if process_csv_file(conn, Path(file_path), table_name):
                success_count += 1
                
    conn.close()
    
    if total_files == 0:
        logger.warning(f"수집된 폴더에 {target_date} {target_venue} 관련 CSV 파일이 전혀 없습니다.")
    else:
        logger.info(f"총 {total_files}개의 파일 중 {success_count}개 파일 업로드 완료.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Mode 1 결과 CSV DB 업로드")
    parser.add_argument("--date", required=True, help="YYYYMMDD 포맷의 날짜")
    parser.add_argument("--venue", required=True, help="경기장 한글명 (예: 도쿄)")
    args = parser.parse_args()

    logger.info(f"==== DB 자동 업로드 (과거 결과 -> MariaDB) 시작: {args.date} {args.venue} ====")
    upload_result_csv_to_db(args.date, args.venue)
    logger.info("==== 업로드 프로세스 종료 ====")
