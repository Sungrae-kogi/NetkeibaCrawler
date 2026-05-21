import csv
import json
import pymysql
import re
import requests
from datetime import datetime, timedelta
from pathlib import Path

# 경로 설정
BASE_DIR = Path("c:/Users/비큐리오/PycharmProjects")
INFO_DIR = BASE_DIR / "InformationCrawler"
CONFIG_PATH = BASE_DIR / "config.json"
DB_CONFIG_PATH = BASE_DIR / "DBIntegration" / "db_config.json"
CSV_PATH = INFO_DIR / "data" / "extracted_info.csv"

def load_db_config():
    with open(DB_CONFIG_PATH, "r", encoding="utf-8") as f:
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

def get_rcdate_historical(crawl_time_str, rcday_kanji):
    """수집 시점(crawl_time_str)을 기준으로 해당 주 요일의 날짜를 계산"""
    day_map = {'月': 0, '火': 1, '水': 2, '木': 3, '金': 4, '土': 5, '日': 6}
    target_idx = day_map.get(rcday_kanji)
    if target_idx is None: return None
    
    crawl_dt = datetime.strptime(crawl_time_str, '%Y-%m-%d %H:%M:%S')
    monday = crawl_dt.date() - timedelta(days=crawl_dt.weekday())
    target_date = monday + timedelta(days=target_idx)
    return int(target_date.strftime("%Y%m%d"))

def lookup_hrno(conn, rcdate, meet, rcno, chulno):
    with conn.cursor() as cursor:
        sql = "SELECT HRNO FROM api_entry_sheet_2 WHERE RCDATE = %s AND MEET = %s AND RCNO = %s AND CHULNO = %s LIMIT 1"
        cursor.execute(sql, (rcdate, meet, rcno, chulno))
        res = cursor.fetchone()
        return res['HRNO'] if res else ""

def main():
    if not CSV_PATH.exists():
        print(f"❌ 파일을 찾을 수 없습니다: {CSV_PATH}")
        return

    db_config = load_db_config()
    conn = get_db_connection(db_config)
    
    print(f"📂 CSV 읽기 시작: {CSV_PATH}")
    
    try:
        with open(CSV_PATH, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            new_insert_count = 0
            
            for row in reader:
                category = row.get("CATEGORY", "")
                place = row.get("PLACE", "")
                details = row.get("DETAILS", "")
                crawl_time = row.get("CRAWL_TIME", "")
                
                # 취소 관련 항목만 필터링
                if not any(k in category for k in ["取消", "除外", "中止"]):
                    continue
                
                # 파싱
                m_place = re.match(r"^(.)曜(.+?)(\d+)R", place)
                if not m_place: continue
                
                rcday = m_place.group(1)
                meet = m_place.group(2)
                rcno = m_place.group(3)
                
                fixed_details = details.replace('\xa0', ' ')
                m_details = re.search(r"(\d+)番\s*([^\(]+)", fixed_details)
                if not m_details: continue
                
                chulno = m_details.group(1)
                hrname = m_details.group(2).strip()
                
                # 날짜 계산 (과거 시점 기준)
                rcdate = get_rcdate_historical(crawl_time, rcday)
                if not rcdate: continue
                
                # HRNO 조회
                hrno = lookup_hrno(conn, rcdate, meet, rcno, chulno)
                
                # DB 삽입 (이름 수정을 위해 ON DUPLICATE KEY UPDATE 사용)
                with conn.cursor() as cursor:
                    sql = """
                        INSERT INTO api_race_horse_cancel_info_1 
                        (CHULNO, HRNAME, HRNO, MEET, RCDATE, RCNO, REASON) 
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE HRNAME = VALUES(HRNAME)
                    """
                    cursor.execute(sql, (chulno, hrname, hrno, meet, rcdate, rcno, category))
                    affected = cursor.rowcount
                
                if affected == 1:
                    new_insert_count += 1
                    conn.commit()
                    print(f"✅ 신규 삽입: {rcdate} {meet} {rcno}R {hrname} (HRNO: {hrno})")
                    
                    # 외부 API 호출 테스트
                    try:
                        deploy_url = f"https://j.mafeel.ai/schedule/deploy/cancelHorse.do?meet={meet}"
                        print(f"🚀 외부 API 호출 시도: {deploy_url}")
                        resp = requests.get(deploy_url, timeout=15)
                        print(f"✅ 외부 API 응답: {resp.status_code} - {resp.text[:50]}")
                    except Exception as e:
                        print(f"⚠️ 외부 API 호출 중 오류: {e}")
                elif affected == 2:
                    conn.commit()
                    print(f"🔄 이름 업데이트: {rcdate} {meet} {rcno}R {hrname}")
                else:
                    print(f"⏭️ 중복 패스: {rcdate} {meet} {chulno}R {hrname}")
            
            print(f"\n✨ 재처리 완료: 총 {new_insert_count}건의 새로운 데이터가 DB에 반영되었습니다.")
            
    finally:
        conn.close()

if __name__ == "__main__":
    main()
