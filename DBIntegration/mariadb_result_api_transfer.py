import os
import sys
import json
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

def execute_result_transfer(target_date_raw, target_venue, max_retries=3):
    # 날짜 포맷 변환 (20260425 -> 2026-04-25)
    target_date = f"{target_date_raw[:4]}-{target_date_raw[4:6]}-{target_date_raw[6:]}"
    
    queries = {
        "1. api_race_detail_result_1 삽입": f"""
            INSERT INTO api_race_detail_result_1 (
                RCDIST, CHULNO, HRNAME, HRNO, JKNAME, JKNO,
                MEET, RCDATE, RCNO, RCTIME, RANK,
                RCTYPE, RCDIRECTION,
                T01, T02, T03, T04, T05, T06, T07, T08, T09, T10,
                T11, T12, T13, T14, T15, T16, T17, T18, T19, T20
            )
            SELECT 
                CAST(NULLIF(TRIM(RCDIST), '') AS UNSIGNED), 
                CAST(NULLIF(TRIM(CHULNO), '') AS UNSIGNED), 
                HRNAME, 
                HRNO, 
                JKNAME, 
                JKNO, 
                MEET, 
                
                CAST(NULLIF(REPLACE(RCDATE, '-', ''), '') AS UNSIGNED), 
                CAST(NULLIF(TRIM(RIGHT(RCNO, 2)), '') AS UNSIGNED), 
                
                -- [어제 로직 복원] RCTIME: '1:34.5' -> 94.5초 완벽 변환
                IF(RACE_RCD LIKE '%:%',
                    (CAST(SUBSTRING_INDEX(RACE_RCD, ':', 1) AS UNSIGNED) * 60) 
                    + CAST(SUBSTRING_INDEX(RACE_RCD, ':', -1) AS FLOAT),
                    CAST(NULLIF(TRIM(RACE_RCD), '') AS FLOAT)
                ), 
                
                RK, 
                TRACK_TYPE, 
                DIRECTION,
                
                CAST(NULLIF(TRIM(distance1), '') AS FLOAT),
                CAST(NULLIF(TRIM(distance2), '') AS FLOAT),
                CAST(NULLIF(TRIM(distance3), '') AS FLOAT),
                CAST(NULLIF(TRIM(distance4), '') AS FLOAT),
                CAST(NULLIF(TRIM(distance5), '') AS FLOAT),
                CAST(NULLIF(TRIM(distance6), '') AS FLOAT),
                CAST(NULLIF(TRIM(distance7), '') AS FLOAT),
                CAST(NULLIF(TRIM(distance8), '') AS FLOAT),
                CAST(NULLIF(TRIM(distance9), '') AS FLOAT),
                CAST(NULLIF(TRIM(distance10), '') AS FLOAT),
                CAST(NULLIF(TRIM(distance11), '') AS FLOAT),
                CAST(NULLIF(TRIM(distance12), '') AS FLOAT),
                CAST(NULLIF(TRIM(distance13), '') AS FLOAT),
                CAST(NULLIF(TRIM(distance14), '') AS FLOAT),
                CAST(NULLIF(TRIM(distance15), '') AS FLOAT),
                CAST(NULLIF(TRIM(distance16), '') AS FLOAT),
                CAST(NULLIF(TRIM(distance17), '') AS FLOAT),
                CAST(NULLIF(TRIM(distance18), '') AS FLOAT),
                CAST(NULLIF(TRIM(distance19), '') AS FLOAT),
                CAST(NULLIF(TRIM(distance20), '') AS FLOAT)
            FROM test_tmp_races
            WHERE RCDATE = '{target_date}' 
              AND MEET IN ('{target_venue}')
            ON DUPLICATE KEY UPDATE
                RCDIST = VALUES(RCDIST),
                HRNAME = VALUES(HRNAME),
                HRNO = VALUES(HRNO),
                JKNAME = VALUES(JKNAME),
                JKNO = VALUES(JKNO),
                RCTIME = VALUES(RCTIME),
                RANK = VALUES(RANK),
                RCTYPE = VALUES(RCTYPE),
                RCDIRECTION = VALUES(RCDIRECTION),
                T01 = VALUES(T01), T02 = VALUES(T02), T03 = VALUES(T03), T04 = VALUES(T04), T05 = VALUES(T05),
                T06 = VALUES(T06), T07 = VALUES(T07), T08 = VALUES(T08), T09 = VALUES(T09), T10 = VALUES(T10),
                T11 = VALUES(T11), T12 = VALUES(T12), T13 = VALUES(T13), T14 = VALUES(T14), T15 = VALUES(T15),
                T16 = VALUES(T16), T17 = VALUES(T17), T18 = VALUES(T18), T19 = VALUES(T19), T20 = VALUES(T20);
        """,
        "2. api_race_result 삽입": f"""
            INSERT INTO api_race_result (
                RCCRS_NM, RACE_DT, RACE_NO, RACE_DS, RACE_NM,
                GTNO, HRNO, HRNM, ENG_HRNM, PCTY_NM,
                BTHD, LATST_PTIN_DT, GNDR_NM, RATG_SO, RCHR_WEG,
                BURD_WGT, JCKY_NM, ENG_JCKY_NM, TRAR_NM, ENG_TRAR_NM,
                OWNER_NM, ENG_OWNER_NM, RK, RACE_RCD, MARGIN,
                WIN_PRICE, PLACE_PRICE
            )
            SELECT 
                r.MEET, 
                CAST(NULLIF(REPLACE(r.RCDATE, '-', ''), '') AS UNSIGNED), 
                CAST(NULLIF(TRIM(RIGHT(r.RCNO, 2)), '') AS UNSIGNED), 
                CAST(NULLIF(TRIM(r.RCDIST), '') AS UNSIGNED), 
                r.RCNAME, 
                CAST(NULLIF(TRIM(r.CHULNO), '') AS UNSIGNED), 
                r.HRNO, 
                r.HRNAME, 
                h.ENG_HRNM, 
                NULL, 
                h.BIRTHDAY, 
                NULL, 
                r.SEX, 
                NULL, 
                r.RCHR_WEG, 
                CAST(NULLIF(TRIM(r.WGBUDAM), '') AS FLOAT), 
                r.JKNAME, 
                NULL, 
                r.TRNAME, 
                NULL, 
                NULL, 
                NULL, 
                r.RK, 
                IF(r.RACE_RCD LIKE '%:%',
                    (CAST(SUBSTRING_INDEX(r.RACE_RCD, ':', 1) AS UNSIGNED) * 60) 
                    + CAST(SUBSTRING_INDEX(r.RACE_RCD, ':', -1) AS FLOAT),
                    CAST(NULLIF(TRIM(r.RACE_RCD), '') AS FLOAT)
                ), 
                r.MARGIN, 
                CAST(NULLIF(TRIM(r.WIN_ODDS), '') AS FLOAT), 
                NULL 
            FROM tmp_races r
            LEFT JOIN tmp_horses h 
                ON r.MEET = h.MEET AND r.HRNO = h.HR_NO
            WHERE r.RCDATE = '{target_date}'
              AND r.MEET IN ('{target_venue}')
              AND r.AGECOND NOT LIKE '障害%'
            ON DUPLICATE KEY UPDATE
                BTHD = VALUES(BTHD),
                RACE_DS = VALUES(RACE_DS),
                RACE_NM = VALUES(RACE_NM),
                HRNM = VALUES(HRNM),
                ENG_HRNM = VALUES(ENG_HRNM),
                GNDR_NM = VALUES(GNDR_NM),
                RCHR_WEG = VALUES(RCHR_WEG),
                BURD_WGT = VALUES(BURD_WGT),
                JCKY_NM = VALUES(JCKY_NM),
                TRAR_NM = VALUES(TRAR_NM),
                RK = VALUES(RK),
                RACE_RCD = VALUES(RACE_RCD),
                MARGIN = VALUES(MARGIN),
                WIN_PRICE = VALUES(WIN_PRICE);
        """
    }

    config = load_db_config()
    
    for attempt in range(1, max_retries + 1):
        try:
            conn = get_db_connection(config)
        except Exception as e:
            if attempt < max_retries:
                wait_time = 2 ** attempt
                logger.warning(f"DB 연결 실패. {wait_time}초 후 재시도... ({attempt}/{max_retries})")
                time.sleep(wait_time)
                continue
            else:
                logger.error(f"DB 연결 최종 실패: {e}")
                return

        logger.info(f"[{target_date} {target_venue}] 과거 경기 결과 이관(JOIN) 쿼리 실행을 시작합니다... (시도: {attempt}/{max_retries})")
        
        success = True
        try:
            with conn.cursor() as cursor:
                for query_name, sql in queries.items():
                    logger.info(f"▶ 실행 중: {query_name}")
                    cursor.execute(sql)
                    logger.info(f"   ㄴ 완료. (영향받은 행: {cursor.rowcount})")
            conn.commit()
        except pymysql.MySQLError as e:
            conn.rollback()
            error_code = e.args[0]
            if error_code in (1213, 1205, 2006, 2013) and attempt < max_retries:
                wait_time = 2 ** attempt
                logger.warning(f"DB 일시적 오류(코드:{error_code}) 발생. 롤백 후 {wait_time}초 뒤 전체 재시도... ({attempt}/{max_retries})")
                success = False
                time.sleep(wait_time)
            else:
                logger.error(f"❌ 쿼리 실행 최종 실패. 에러 내역: {e}")
                conn.close()
                return
        except Exception as e:
            conn.rollback()
            logger.error(f"❌ 예상치 못한 오류 발생: {e}")
            conn.close()
            return
            
        conn.close()
        if success:
            logger.info("🎉 과거 결과 데이터 API 테이블 이관 작업이 성공적으로 완료되었습니다!")
            return

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Mode 1 결과 데이터 API 이관 (JOIN)")
    parser.add_argument("--date", required=True, help="YYYYMMDD 포맷의 날짜")
    parser.add_argument("--venue", required=True, help="경기장 한글명 (예: 도쿄)")
    args = parser.parse_args()
    
    logger.info(f"==== DB API 테이블 데이터 이관(결과용) 모듈 시작: {args.date} {args.venue} ====")
    execute_result_transfer(args.date, args.venue)
    logger.info("==== 이관 프로세스 종료 ====")
