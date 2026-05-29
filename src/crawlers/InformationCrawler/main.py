import csv
import json
import time
import hashlib
import requests
import re
import os
import logging
import pymysql
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from pathlib import Path
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
LOG_DIR = BASE_DIR.parent.parent.parent / "logs"
CACHE_FILE = DATA_DIR / "seen_cache.json"

# 날짜별 파일명 생성
date_str = datetime.now().strftime("%Y%m%d")
CSV_FILE = DATA_DIR / f"extracted_info_{date_str}.csv"
CANCEL_CSV_FILE = DATA_DIR / f"cancel_extracted_info_{date_str}.csv"
LOG_FILE = LOG_DIR / f"{date_str}_Information.log"

# 로깅 설정
LOG_DIR.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("Information")
URL = "https://race.netkeiba.com/top/information.html?rf=sidemenu"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)"
}

# 재시도 전략 설정
retry_strategy = Retry(
    total=3,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET"]
)
adapter = HTTPAdapter(max_retries=retry_strategy)
http = requests.Session()
http.mount("https://", adapter)
http.mount("http://", adapter)
def load_cache():
    """기존 파싱된 정보의 해시값 목록을 불러옵니다."""
    if CACHE_FILE.exists():
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            return set(json.load(f))
    return set()

def save_cache(cache_set):
    """현재까지 파싱된 정보의 해시값 목록을 캐시 파일에 저장합니다."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(list(cache_set), f, ensure_ascii=False, indent=2)

def generate_hash(text):
    """문자열에 대한 MD5 해시값을 생성합니다."""
    return hashlib.md5(text.encode('utf-8')).hexdigest()

def load_db_config():
    """DBIntegration/db_config.json에서 설정 로드"""
    import os
    env = os.environ.get("APP_ENV", "prod")
    config_name = "db_config_test.json" if env == "test" else "db_config.json"
    config_path = BASE_DIR.parent.parent.parent / "config" / config_name
    if not config_path.exists():
        logger.error(f"DB 설정 파일을 찾을 수 없습니다: {config_path}")
        return None
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)

def get_db_connection(config):
    """MariaDB 연결 생성"""
    try:
        return pymysql.connect(
            host=config['host'],
            port=config['port'],
            user=config['user'],
            password=config['password'],
            database=config['database'],
            charset=config.get('charset', 'utf8mb4'),
            cursorclass=pymysql.cursors.DictCursor
        )
    except Exception as e:
        logger.error(f"DB 연결 중 오류 발생: {e}")
        return None

def send_telegram_message(message):
    """config.json 설정을 참조하여 텔레그램 메시지 발송"""
    config_path = BASE_DIR.parent.parent.parent / "config" / "config.json"
    if not config_path.exists():
        return
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            config = json.load(f)
        bot_token = config.get("TELEGRAM_BOT_TOKEN")
        chat_id = config.get("TELEGRAM_CHAT_ID")
        if bot_token and chat_id:
            url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
            payload = {"chat_id": chat_id, "text": message}
            requests.post(url, json=payload, timeout=10)
    except Exception as e:
        logger.error(f"텔레그램 발송 중 오류: {e}")

def get_rcdate_from_day(rcday_kanji):
    """요일(土, 日 등)을 바탕으로 이번 주 실제 날짜(YYYYMMDD)를 계산합니다."""
    day_map = {'月': 0, '火': 1, '水': 2, '木': 3, '金': 4, '土': 5, '日': 6}
    target_idx = day_map.get(rcday_kanji)
    if target_idx is None:
        return None
    
    today = datetime.now().date()
    # 이번 주 월요일 계산
    monday = today - timedelta(days=today.weekday())
    target_date = monday + timedelta(days=target_idx)
    return int(target_date.strftime("%Y%m%d"))

def lookup_hrno(conn, rcdate, meet, rcno, chulno):
    """api_entry_sheet_2 테이블에서 RCDATE, MEET, RCNO, CHULNO로 HRNO를 조회합니다."""
    try:
        with conn.cursor() as cursor:
            sql = """
                SELECT HRNO FROM api_entry_sheet_2 
                WHERE RCDATE = %s AND MEET = %s AND RCNO = %s AND CHULNO = %s 
                LIMIT 1
            """
            cursor.execute(sql, (rcdate, meet, rcno, chulno))
            result = cursor.fetchone()
            return result['HRNO'] if result else ""
    except Exception as e:
        logger.error(f"HRNO 조회 중 오류: {e}")
        return ""

def sync_cancel_to_db(records):
    """파싱된 레코드를 DB에 적재하고 필요 시 알림을 보냅니다."""
    db_config = load_db_config()
    if not db_config:
        return
    
    conn = get_db_connection(db_config)
    if not conn:
        return
    
    try:
        # 1. MEET 기준으로 데이터 분류 (그룹화)
        grouped_records = {}
        for rec in records:
            meet = rec.get("MEET")
            if not meet:
                continue
            if meet not in grouped_records:
                grouped_records[meet] = []
            grouped_records[meet].append(rec)
            
        total_new_insert_count = 0
        
        # 2. 그룹별 순회 및 DB 적재
        for meet, meet_records in grouped_records.items():
            meet_new_insert_count = 0
            
            for rec in meet_records:
                rcdate = rec.get("RCDATE")
                rcno = rec.get("RCNO")
                chulno = rec.get("CHULNO")
                hrname = rec.get("HRNAME")
                category = rec.get("CATEGORY")
                
                # HRNO 조회
                hrno = lookup_hrno(conn, rcdate, meet, rcno, chulno)
                
                # DB 삽입 (이름/사유 등이 바뀔 수 있으므로 UPDATE 처리)
                with conn.cursor() as cursor:
                    sql = """
                        INSERT INTO api_race_horse_cancel_info_1 
                        (CHULNO, HRNAME, HRNO, MEET, RCDATE, RCNO, REASON) 
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE 
                            HRNAME = VALUES(HRNAME),
                            MEET = VALUES(MEET),
                            REASON = VALUES(REASON)
                    """
                    cursor.execute(sql, (chulno, hrname, hrno, meet, rcdate, rcno, category))
                    affected = cursor.rowcount
                
                if affected == 1:
                    logger.info(f"💾 [DB 응답] affected: {affected} - (신규 삽입) DB에 없던 새로운 데이터라서 INSERT 되었습니다.")
                    meet_new_insert_count += 1
                    total_new_insert_count += 1
                    conn.commit()
                    
                    # 텔레그램 발송 (건별 상세 알림 유지)
                    msg = f"🚩 [신규 취소/제외/중지 정보]\n\n날짜: {rcdate}\n경주: {meet} {rcno}R\n마번: {chulno}번 ({hrname})\n구분: {category}"
                    logger.info(f"DB 신규 적재 완료 및 알림 발송: {hrname} ({rcdate})")
                    send_telegram_message(msg)
                    
                elif affected == 2:
                    logger.info(f"💾 [DB 응답] affected: {affected} - (수정됨) 이미 DB에 존재하지만 일부 값(이름/사유 등)이 달라서 UPDATE 되었습니다.")
                    conn.commit()
                    logger.info(f"🔄 기존 정보 수정됨 (알림 미발송): {hrname} ({rcdate})")
                else:
                    logger.info(f"💾 [DB 응답] affected: {affected} - (변화 없음) 이미 DB에 존재하고 모든 값이 완벽히 동일하여 무시(No-op)되었습니다.")
                    logger.info(f"⏭️ 중복 패스 (변화 없음): {rcdate} {meet} {rcno}R {hrname}")
            
            # 3. MEET별 처리가 끝난 후, 카운트 확인 후 외부 API 1회 발송
            if meet_new_insert_count > 0:
                try:
                    deploy_url = f"http://j.mafeel.ai/schedule/deploy/cancelHorse.do?meet={meet}"
                    logger.info(f"🚀 외부 API 호출 시도 (MEET 단위 일괄 1회 발송, 최대 10분 대기): {deploy_url}")
                    resp = requests.get(deploy_url, timeout=600)
                    
                    logger.info(f"📊 [API 응답 상세] 상태코드: {resp.status_code}")
                    logger.info(f"📊 [API 응답 상세] 소요시간: {resp.elapsed.total_seconds():.2f}초")
                    logger.info(f"📊 [API 응답 상세] 헤더: {dict(resp.headers)}")
                    logger.info(f"📊 [API 응답 상세] 본문(Text): {resp.text[:1000]}") 
                    
                    try:
                        resp_json = resp.json()
                        if resp.status_code == 200 and resp_json.get("result") == "OK":
                            logger.info(f"✅ 외부 API 호출 완벽 성공! ({meet} 처리 완료)")
                        else:
                            logger.warning(f"⚠️ API는 호출되었으나 반환값이 OK가 아닙니다. ({meet})")
                    except ValueError:
                        if resp.status_code == 200:
                            logger.warning(f"✅ API 상태는 200이나, JSON 응답(result=OK)이 아닙니다. ({meet})")
                        else:
                            logger.warning(f"⚠️ 외부 API 호출 실패 ({meet})")
                            
                except requests.exceptions.Timeout:
                    logger.error(f"🚨 [치명적 경고] 외부 API 호출 중 타임아웃(10분 초과) 발생 ({meet})!")
                except Exception as e:
                    logger.error(f"🚨 외부 API 호출 중 오류 발생 ({meet}): {e}")

        if total_new_insert_count > 0:
            logger.info(f"✅ 총 {total_new_insert_count}건의 신규 데이터가 DB에 적재되었습니다.")
            
    except Exception as e:
        logger.error(f"DB 동기화 중 오류 발생: {e}")
        conn.rollback()
    finally:
        conn.close()

def fetch_and_parse():
    logger.info("넷케이바 Information 갱신 내역 확인 중...")
    try:
        r = http.get(URL, headers=HEADERS, timeout=15)
        # 넷케이바 인코딩 보정
        r.encoding = "EUC-JP"
        r.raise_for_status()
    except Exception as e:
        logger.error(f"페이지 구조를 가져오는데 실패했습니다 (재시도 후 최종 실패): {e}")
        return

    soup = BeautifulSoup(r.text, "lxml")
    
    # 해당 사이트는 클래스명에 오타(Infomation)가 있음.
    # 혹시 나중에 고쳐질 수도 있으니 두 가지 경우를 모두 대비
    items = soup.select("div.Race_Information ul.Infomation > li")
    if not items:
        items = soup.select("div.Race_Information ul.Information > li")
    
    seen_cache = load_cache()
    new_records = []
    
    for li in items:
        # 카테고리 (출주 취소, 기수 변경 등)
        cat_elem = li.select_one("dt.Link_Title span")
        # 대상 경기/장소 (예: 토요 나카야마 6R)
        place_elem = li.select_one("dt.Link_Title div")
        # 내용 및 업로드 시간 
        sub_txt = li.select_one("dd.Sub_Txt")
        
        category = cat_elem.text.strip() if cat_elem else ""
        place = place_elem.text.strip() if place_elem else ""
        details = sub_txt.text.strip() if sub_txt else ""
        
        # 고유 식별자 생성
        combined_text = f"{category}|{place}|{details}"
        item_hash = generate_hash(combined_text)
        
        # 처음 보는 정보(해시)라면 새로 등록
        if item_hash not in seen_cache:
            seen_cache.add(item_hash)
            new_records.append({
                "CRAWL_TIME": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                "CATEGORY": category,
                "PLACE": place,
                "DETAILS": details,
                "HASH_ID": item_hash
            })
            
    if new_records:
        logger.info(f"🎉 새로운 정보 {len(new_records)}건이 발견되어 저장합니다!")
        save_csv(new_records)
        
        # [데이터 분리 추출] 취소/제외/중지 항목만 따로 파싱하여 저장
        cancel_records = []
        for rec in new_records:
            if any(k in rec["CATEGORY"] for k in ["取消", "除外", "中止"]):
                cancel_records.append(parse_cancel_record(rec))
                
        if cancel_records:
            save_cancel_csv(cancel_records)
            logger.info(f"     (취소/제외/중지 데이터 {len(cancel_records)}건 파싱 완료)")
            # 실시간 DB 동기화 실행
            sync_cancel_to_db(cancel_records)
            
        save_cache(seen_cache)
        for rec in reversed(new_records):
            logger.info(f"     [{rec['CATEGORY']}] {rec['PLACE']} - {rec['DETAILS']}")
    else:
        logger.info("새로운 업데이트가 없습니다.")

def save_csv(records):
    """새로 감지된 정보들을 CSV에 누적(Append)하여 저장합니다."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    file_exists = CSV_FILE.exists()
    
    fieldnames = ["CRAWL_TIME", "CATEGORY", "PLACE", "DETAILS", "HASH_ID"]
    
    with open(CSV_FILE, "a", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        # 파일이 없었다면 헤더 먼저 작성
        if not file_exists:
            writer.writeheader()
        
        # 위에서부터 파싱하면 최신 데이터가 먼저 들어옴.
        # 기록은 시간 순서(과거->최신)로 남기기 위해 뒤집어서 저장
        for row in reversed(records):
            writer.writerow(row)

def parse_cancel_record(record):
    """취소/제외/중지 타입의 데이터를 정규식으로 분해하여 새 구조로 반환합니다."""
    out = {
        "CRAWL_TIME": record["CRAWL_TIME"],
        "CATEGORY": record["CATEGORY"],
        "RCDAY": "",
        "MEET": "",
        "RCNO": "",
        "CHULNO": "",
        "HRNAME": "",
        "RCDATE": ""
    }
    
    place = record.get("PLACE", "")
    details = record.get("DETAILS", "")
    
    # PLACE 파싱: 예) 土曜阪神6R -> RCDAY=土, MEET=阪神, RCNO=6
    m_place = re.match(r"^(.)曜(.+?)(\d+)R", place)
    if m_place:
        out["RCDAY"] = m_place.group(1)
        out["MEET"] = m_place.group(2)
        out["RCNO"] = m_place.group(3)
        
    # DETAILS 파싱: 예) 2番 エイコーンドリーム (4/18 12:43) 
    # -> CHULNO=2, HRNAME=エイコーンドリーム
    fixed_details = details.replace('\xa0', ' ')
    # reprocess_old_csv.py에서 검증된 더 유연한 정규식 사용
    m_details = re.search(r"(\d+)番\s*([^\(]+)", fixed_details)
    if m_details:
        out["CHULNO"] = m_details.group(1)
        out["HRNAME"] = m_details.group(2).strip()
        
    # RCDATE 계산 (RCDAY 요일 기준)
    if out["RCDAY"]:
        calculated_date = get_rcdate_from_day(out["RCDAY"])
        if calculated_date:
            out["RCDATE"] = calculated_date
            
    return out

def save_cancel_csv(records):
    """파싱된 취소/제외/중지 데이터를 별도의 CSV(cancel_extracted_info.csv)에 누적 저장합니다."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    file_exists = CANCEL_CSV_FILE.exists()
    
    fieldnames = ["CRAWL_TIME", "CATEGORY", "RCDAY", "MEET", "RCNO", "CHULNO", "HRNAME", "RCDATE"]
    
    with open(CANCEL_CSV_FILE, "a", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        
        for row in reversed(records):
            writer.writerow(row)

def main():
    logger.info("==============================================================")
    logger.info("🐎 Netkeiba Information 단발성(Cron/스케줄러) 수집 시작")
    logger.info("==============================================================\n")
    
    flag_file = BASE_DIR / "flag.txt"
    
    # 1. Lock 파일 존재 여부 및 타임스탬프 기반 Stale Lock 체크
    if flag_file.exists():
        try:
            with open(flag_file, "r") as f:
                content = f.read().strip()
                started_at = float(content) if content else 0.0
                
            if time.time() - started_at > 1800:  # 30분 초과
                logger.warning("🚨 [Stale Lock 감지] 이전 작업이 30분 이상 지연되어 비정상 종료로 간주하고 Lock을 해제합니다.")
                os.remove(flag_file)
            else:
                logger.info("⏳ 이전 주기의 스크립트가 아직 정상 실행 중입니다. 중복 실행을 방지하기 위해 이번 주기를 스킵(종료)합니다.")
                return
        except Exception as e:
            logger.error(f"flag.txt 읽기 중 오류 발생 (안전을 위해 강제 삭제 후 진행 시도): {e}")
            try:
                os.remove(flag_file)
            except:
                pass
                
    # 2. Lock 파일 생성
    try:
        with open(flag_file, "w") as f:
            f.write(str(time.time()))
    except Exception as e:
        logger.error(f"flag.txt 생성 실패! 동시성 제어가 보장되지 않습니다: {e}")
    
    # 3. 데이터 수집 본 작업 실행
    try:
        fetch_and_parse()
        logger.info("\n✅ [안내] 수집 및 처리 작업이 정상적으로 완료되었습니다.")
    except Exception as e:
        logger.error(f"❌ [에러] 예기치 못한 시스템 오류 발생: {e}")
        
    # 4. Lock 파일 삭제 (정상/비정상 무관, 사용자의 단일 흐름 요청 적용)
    try:
        if flag_file.exists():
            os.remove(flag_file)
    except Exception as e:
        logger.error(f"flag.txt 삭제 중 오류 발생: {e}")

if __name__ == "__main__":
    main()
