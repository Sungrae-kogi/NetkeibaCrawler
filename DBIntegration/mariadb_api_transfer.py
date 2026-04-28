import os
import sys
import json
import logging
import time
from pathlib import Path
import pymysql

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("API_Transfer")

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

def execute_transfer(max_retries=3):
    if str(BASE_DIR.parent) not in sys.path:
        sys.path.append(str(BASE_DIR.parent))
    from WebCrawler.discovery import get_all_target_races

    targets_info = get_all_target_races()
    if not targets_info:
        logger.warning("자동 탐색(discovery)된 경기 정보가 없습니다. DB 이관을 종료합니다.")
        return

    unique_venues = set(t['venue'] for t in targets_info)
    unique_dates = set(t['date'] for t in targets_info)
    
    target_venues = "(" + ", ".join(f"'{v}'" for v in unique_venues) + ")"
    
    formatted_dates = []
    for d in unique_dates:
        formatted_dates.append(f"'{d[:4]}-{d[4:6]}-{d[6:]}'")
    target_dates = "(" + ", ".join(formatted_dates) + ")"
    
    queries = {
        "1. api_race_horse_info_2 삽입": f"""
            INSERT INTO api_race_horse_info_2 (
                `MEET`, `HR_NAME`, `HR_NO`, `NAME`, `SEX`, `BIRTHDAY`, `RANK`, 
                `TR_NAME`, `TR_NO`, `OW_NAME`, `OW_NO`, 
                `FA_HR_NAME`, `FA_HR_NO`, `MO_HR_NAME`, `MO_HR_NO`, 
                `RC_CNTT`, `ORD1_CNTT`, `ORD2_CNTT`, `ORD3_CNTT`, 
                `RC_CNTY`, `ORD1_CNTY`, `ORD2_CNTY`, `ORD3_CNTY`, 
                `CHAKSUNT`, `RATING`, `HR_LAST_AMT`
            )
            SELECT 
                `MEET`, `HR_NAME`, `HR_NO`, `BRED_REGION`, `SEX`, 
                CAST(DATE_FORMAT(STR_TO_DATE(NULLIF(TRIM(`BIRTHDAY`), ''), '%Y年%c月%e日'), '%Y%m%d') AS UNSIGNED), 
                NULL,
                `TR_NAME`, `TR_NO`, `OW_NAME`, `OW_NO`, 
                `FA_HR_NAME`, `FA_HR_NO`, `MO_HR_NAME`, `MO_HR_NO`, 
                CAST(NULLIF(REGEXP_REPLACE(`RC_CNTT`, '[^0-9]', ''), '') AS UNSIGNED),
                CAST(NULLIF(REGEXP_REPLACE(`ORD1_CNTT`, '[^0-9]', ''), '') AS UNSIGNED),
                CAST(NULLIF(REGEXP_REPLACE(`ORD2_CNTT`, '[^0-9]', ''), '') AS UNSIGNED),
                CAST(NULLIF(REGEXP_REPLACE(`ORD3_CNTT`, '[^0-9]', ''), '') AS UNSIGNED),
                CAST(NULLIF(REGEXP_REPLACE(`RC_CNTY`, '[^0-9]', ''), '') AS UNSIGNED),
                CAST(NULLIF(REGEXP_REPLACE(`ORD1_CNTY`, '[^0-9]', ''), '') AS UNSIGNED),
                CAST(NULLIF(REGEXP_REPLACE(`ORD2_CNTY`, '[^0-9]', ''), '') AS UNSIGNED),
                CAST(NULLIF(REGEXP_REPLACE(`ORD3_CNTY`, '[^0-9]', ''), '') AS UNSIGNED),
                (IFNULL(CAST(NULLIF(TRIM(`CHAKSUNT_JRA`), '') AS UNSIGNED), 0) + 
                 IFNULL(CAST(NULLIF(TRIM(`CHAKSUNT_NAR`), '') AS UNSIGNED), 0)),
                NULL,
                `HR_LAST_AMT`
            FROM tmp_horses
            WHERE MEET IN {target_venues}
            ON DUPLICATE KEY UPDATE
                `HR_NAME`    = VALUES(`HR_NAME`),
                `NAME`       = VALUES(`NAME`),
                `SEX`        = VALUES(`SEX`),
                `BIRTHDAY`   = VALUES(`BIRTHDAY`),
                `TR_NAME`    = VALUES(`TR_NAME`),
                `TR_NO`      = VALUES(`TR_NO`),
                `OW_NAME`    = VALUES(`OW_NAME`),
                `OW_NO`      = VALUES(`OW_NO`),
                `FA_HR_NAME` = VALUES(`FA_HR_NAME`),
                `FA_HR_NO`   = VALUES(`FA_HR_NO`),
                `MO_HR_NAME` = VALUES(`MO_HR_NAME`),
                `MO_HR_NO`   = VALUES(`MO_HR_NO`),
                `RC_CNTT`    = VALUES(`RC_CNTT`),
                `ORD1_CNTT`  = VALUES(`ORD1_CNTT`),
                `ORD2_CNTT`  = VALUES(`ORD2_CNTT`),
                `ORD3_CNTT`  = VALUES(`ORD3_CNTT`),
                `RC_CNTY`    = VALUES(`RC_CNTY`),
                `ORD1_CNTY`  = VALUES(`ORD1_CNTY`),
                `ORD2_CNTY`  = VALUES(`ORD2_CNTY`),
                `ORD3_CNTY`  = VALUES(`ORD3_CNTY`),
                `CHAKSUNT`   = VALUES(`CHAKSUNT`),
                `HR_LAST_AMT` = VALUES(`HR_LAST_AMT`);
        """,
        "2. api_entry_sheet_2 삽입": f"""
            INSERT INTO api_entry_sheet_2 (
                MEET, RCDATE, RCDAY, RCNO, WAKU, CHULNO,
                HRNAME, HRNO, PRD, SEX, AGE, HR_LAST_AMT, WGBUDAM,
                RATING, JKNAME, JKNO, TRNAME, TRNO,
                OWNAME, OWNO, ILSU, RCDIST, DUSU,
                RANK, PRIZECOND, AGECOND, STTIME, BUDAM,
                RCNAME, CHAKSUN1, CHAKSUN2, CHAKSUN3, CHAKSUN4, CHAKSUN5,
                CHAKSUNT, CHAKSUNY, CHAKSUN_6M,
                ORD1CNTT, ORD2CNTT, ORD3CNTT, RCCNTT,
                ORD1CNTY, ORD2CNTY, ORD3CNTY, RCCNTY
            )
            SELECT 
                r.MEET, 
                CAST(NULLIF(REPLACE(r.RCDATE, '-', ''), '') AS UNSIGNED), 
                r.RCDAY, 
                CAST(NULLIF(TRIM(RIGHT(r.RCNO, 2)), '') AS UNSIGNED),
                r.WAKU,
                CAST(NULLIF(TRIM(r.CHULNO), '') AS UNSIGNED),
                r.HRNAME, 
                r.HRNO, 
                h.BRED_REGION, 
                r.SEX, 
                CAST(NULLIF(TRIM(r.AGE), '') AS UNSIGNED), 
                CAST(
                    REGEXP_REPLACE(
                        NULLIF(TRIM(h.HR_LAST_AMT), '-'), 
                        '[^0-9]', 
                        ''
                    ) AS UNSIGNED
                ),
                CAST(NULLIF(TRIM(r.WGBUDAM), '') AS FLOAT),
                NULL, 
                r.JKNAME, 
                r.JKNO, 
                r.TRNAME, 
                r.TRNO,
                h.OW_NAME, 
                h.OW_NO, 
                NULL, 
                CAST(NULLIF(TRIM(r.RCDIST), '') AS UNSIGNED), 
                CAST(NULLIF(TRIM(r.DUSU), '') AS UNSIGNED),
                r.RANK, 
                NULL, 
                r.AGECOND, 
                r.STTIME, 
                NULL, 
                r.RCNAME, 
                CAST(NULLIF(TRIM(r.CHAKSUN1), '') AS UNSIGNED), 
                CAST(NULLIF(TRIM(r.CHAKSUN2), '') AS UNSIGNED), 
                CAST(NULLIF(TRIM(r.CHAKSUN3), '') AS UNSIGNED), 
                CAST(NULLIF(TRIM(r.CHAKSUN4), '') AS UNSIGNED), 
                CAST(NULLIF(TRIM(r.CHAKSUN5), '') AS UNSIGNED),
                (IFNULL(CAST(NULLIF(TRIM(h.CHAKSUNT_JRA), '') AS UNSIGNED), 0) + 
                 IFNULL(CAST(NULLIF(TRIM(h.CHAKSUNT_NAR), '') AS UNSIGNED), 0)),
                h.CHAKSUNY,   
                h.CHAKSUN_6M, 
                CAST(NULLIF(TRIM(h.ORD1_CNTT), '') AS UNSIGNED), 
                CAST(NULLIF(TRIM(h.ORD2_CNTT), '') AS UNSIGNED), 
                CAST(NULLIF(TRIM(h.ORD3_CNTT), '') AS UNSIGNED), 
                CAST(NULLIF(TRIM(h.RC_CNTT), '') AS UNSIGNED), 
                CAST(NULLIF(TRIM(h.ORD1_CNTY), '') AS UNSIGNED), 
                CAST(NULLIF(TRIM(h.ORD2_CNTY), '') AS UNSIGNED), 
                CAST(NULLIF(TRIM(h.ORD3_CNTY), '') AS UNSIGNED), 
                CAST(NULLIF(TRIM(h.RC_CNTY), '') AS UNSIGNED)
            FROM tmp_races r
            LEFT JOIN tmp_horses h 
                ON r.MEET = h.MEET AND r.HRNO = h.HR_NO
            WHERE r.RCDATE IN {target_dates};
        """,
        "3. api_race_plan 삽입": f"""
            INSERT INTO api_race_plan (
                RCCRS_NM, RACE_DT, RACE_DY_CNT, RACE_DOTW, RACE_NO,
                RCGRD, RACE_NM, RACE_DS, PTIN_NHR, RACE_CLAS,
                CNDTS_RATG, CNDTS_AG, CNDTS_GNDR, CNDTS_BURD_WGT, CNDTS_NCMR,
                RPM_FPLC, RPM_SPLC, RPM_TPLC, RPM_FOPLC, RPM_FVPLC,
                ADMNY_FPLC, ADMNY_SPLC, ADMNY_TPLC,
                STRT_PARG_TM, STRT_TM,
                WETR, GOING, RCTYPE, RCDIRECTION
            )
            SELECT 
                MEET, 
                CAST(NULLIF(REPLACE(RCDATE, '-', ''), '') AS UNSIGNED), 
                NULL, 
                MAX(RCDAY), 
                CAST(NULLIF(TRIM(RIGHT(RCNO, 2)), '') AS UNSIGNED),
                MAX(RANK), 
                MAX(RCNAME), 
                CAST(NULLIF(TRIM(MAX(RCDIST)), '') AS UNSIGNED), 
                CAST(NULLIF(TRIM(MAX(DUSU)), '') AS UNSIGNED), 
                MAX(RANK), 
                NULL, 
                MAX(AGECOND), 
                NULL, 
                NULL, 
                NULL, 
                MAX(CHAKSUN1), MAX(CHAKSUN2), MAX(CHAKSUN3), 
                MAX(CHAKSUN4), MAX(CHAKSUN5),
                NULL, NULL, NULL, 
                MAX(STTIME), MAX(STTIME),
                MAX(WETR), MAX(GOING), MAX(TRACK_TYPE), MAX(DIRECTION)
            FROM tmp_races
            WHERE RCDATE IN {target_dates}
            GROUP BY 
                MEET, RCDATE, RCNO
            ON DUPLICATE KEY UPDATE
                RACE_DOTW = VALUES(RACE_DOTW),
                RCGRD = VALUES(RCGRD),
                RACE_NM = VALUES(RACE_NM),
                RACE_DS = VALUES(RACE_DS),
                PTIN_NHR = VALUES(PTIN_NHR),
                RACE_CLAS = VALUES(RACE_CLAS),
                CNDTS_AG = VALUES(CNDTS_AG),
                RPM_FPLC = VALUES(RPM_FPLC),
                RPM_SPLC = VALUES(RPM_SPLC),
                RPM_TPLC = VALUES(RPM_TPLC),
                RPM_FOPLC = VALUES(RPM_FOPLC),
                RPM_FVPLC = VALUES(RPM_FVPLC),
                STRT_PARG_TM = VALUES(STRT_PARG_TM),
                STRT_TM = VALUES(STRT_TM),
                WETR = VALUES(WETR),
                GOING = VALUES(GOING),
                RCTYPE = VALUES(RCTYPE),
                RCDIRECTION = VALUES(RCDIRECTION);
        """,
        "4. api_totalrecord_1 (경주마) 삽입": f"""
            INSERT INTO api_totalrecord_1 (
                PRGUBUN, MEET, PRNO, PRNAME,
                ORD1CNTT, ORD2CNTT, ORD3CNTT, RCCNTT, CHAKSUNT,
                ORD1CNTY, ORD2CNTY, ORD3CNTY, RCCNTY, CHAKSUNY, 
                CHAKSUN_6M
            )
            SELECT 
                '경주마', 
                MEET, 
                HR_NO, 
                HR_NAME,
                CAST(NULLIF(TRIM(ORD1_CNTT), '') AS UNSIGNED), 
                CAST(NULLIF(TRIM(ORD2_CNTT), '') AS UNSIGNED), 
                CAST(NULLIF(TRIM(ORD3_CNTT), '') AS UNSIGNED), 
                CAST(NULLIF(TRIM(RC_CNTT), '') AS UNSIGNED), 
                (IFNULL(CAST(NULLIF(TRIM(CHAKSUNT_JRA), '') AS UNSIGNED), 0) + 
                 IFNULL(CAST(NULLIF(TRIM(CHAKSUNT_NAR), '') AS UNSIGNED), 0)),
                CAST(NULLIF(TRIM(ORD1_CNTY), '') AS UNSIGNED), 
                CAST(NULLIF(TRIM(ORD2_CNTY), '') AS UNSIGNED), 
                CAST(NULLIF(TRIM(ORD3_CNTY), '') AS UNSIGNED), 
                CAST(NULLIF(TRIM(RC_CNTY), '') AS UNSIGNED), 
                CAST(NULLIF(TRIM(CHAKSUNY), '') AS UNSIGNED),
                CAST(NULLIF(TRIM(CHAKSUN_6M), '') AS UNSIGNED)
            FROM tmp_horses
            WHERE MEET IN {target_venues}
            ON DUPLICATE KEY UPDATE
                PRNAME = VALUES(PRNAME),
                ORD1CNTT = VALUES(ORD1CNTT),
                ORD2CNTT = VALUES(ORD2CNTT),
                ORD3CNTT = VALUES(ORD3CNTT),
                RCCNTT = VALUES(RCCNTT),
                CHAKSUNT = VALUES(CHAKSUNT),
                ORD1CNTY = VALUES(ORD1CNTY),
                ORD2CNTY = VALUES(ORD2CNTY),
                ORD3CNTY = VALUES(ORD3CNTY),
                RCCNTY = VALUES(RCCNTY),
                CHAKSUNY = VALUES(CHAKSUNY),
                CHAKSUN_6M = VALUES(CHAKSUN_6M);
        """,
        "5. api_totalrecord_1 (조교사) 삽입": f"""
            INSERT INTO api_totalrecord_1 (
                PRGUBUN, MEET, PRNO, PRNAME,
                ORD1CNTT, ORD2CNTT, ORD3CNTT, RCCNTT, CHAKSUNT,
                ORD1CNTY, ORD2CNTY, ORD3CNTY, RCCNTY, CHAKSUNY
            )
            SELECT 
                '조교사', 
                MEET, 
                PRNO, 
                PRNAME,
                CAST(NULLIF(TRIM(ORD1CNTT), '') AS UNSIGNED), 
                CAST(NULLIF(TRIM(ORD2CNTT), '') AS UNSIGNED), 
                CAST(NULLIF(TRIM(ORD3CNTT), '') AS UNSIGNED), 
                CAST(NULLIF(TRIM(RCCNTT), '') AS UNSIGNED), 
                CAST(NULLIF(TRIM(CHAKSUNT), '') AS UNSIGNED), 
                CAST(NULLIF(TRIM(ORD1CNTY), '') AS UNSIGNED), 
                CAST(NULLIF(TRIM(ORD2CNTY), '') AS UNSIGNED), 
                CAST(NULLIF(TRIM(ORD3CNTY), '') AS UNSIGNED), 
                CAST(NULLIF(TRIM(RCCNTY), '') AS UNSIGNED), 
                CAST(NULLIF(TRIM(CHAKSUNY), '') AS UNSIGNED)
            FROM tmp_trainers
            where MEET in {target_venues}
            ON DUPLICATE KEY UPDATE
                PRNAME = VALUES(PRNAME),
                ORD1CNTT = VALUES(ORD1CNTT),
                ORD2CNTT = VALUES(ORD2CNTT),
                ORD3CNTT = VALUES(ORD3CNTT),
                RCCNTT = VALUES(RCCNTT),
                CHAKSUNT = VALUES(CHAKSUNT),
                ORD1CNTY = VALUES(ORD1CNTY),
                ORD2CNTY = VALUES(ORD2CNTY),
                ORD3CNTY = VALUES(ORD3CNTY),
                RCCNTY = VALUES(RCCNTY),
                CHAKSUNY = VALUES(CHAKSUNY);
        """
    }
    
    config = load_db_config()
    
    max_retries = 3
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

        logger.info(f"테이블 간 데이터 이관(JOIN) 쿼리 실행을 시작합니다... (시도: {attempt}/{max_retries})")
        
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
            logger.info("🎉 모든 API 테이블 이관 작업이 성공적으로 완료되었습니다!")
            return

if __name__ == "__main__":
    logger.info("==== DB API 테이블 데이터 이관(JOIN) 모듈 시작 ====")
    execute_transfer()
    logger.info("==== 이관 프로세스 종료 ====")
