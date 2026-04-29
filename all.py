import os
import sys
import time
import subprocess
import logging
import re
import requests
import json
from datetime import datetime
from pathlib import Path

# Paths Setup
BASE_DIR = Path(__file__).resolve().parent

# discovery 및 weather 모듈을 시스템 경로에 추가
sys.path.append(str(BASE_DIR))
from WebCrawler.discovery import discover_races, get_all_target_races
from WeatherCrawler.main import run_weather_crawl
from netkeiba_auth import get_netkeiba_cookies

def send_telegram_message(message: str):
    """텔레그램 봇으로 메시지를 전송합니다."""
    config_path = BASE_DIR / "config.json"
    if not config_path.exists():
        return
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            config = json.load(f)
        bot_token = config.get("TELEGRAM_BOT_TOKEN")
        chat_id = config.get("TELEGRAM_CHAT_ID")
        
        if not bot_token or not chat_id:
            return
            
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        payload = {"chat_id": chat_id, "text": message}
        requests.post(url, json=payload, timeout=10)
    except Exception as e:
        logger.error(f"텔레그램 알림 발송 오류: {e}")

LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)
date_str = datetime.now().strftime("%Y%m%d")
LOG_FILE = LOG_DIR / f"{date_str}_Master.log"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("Master")

WEB_CRAWLER_DIR = BASE_DIR / "WebCrawler"
ENTRY_SHEET_DIR = WEB_CRAWLER_DIR / "entry_sheet_2"
HR_DIR = BASE_DIR / "HRNOCrawler"
JK_DIR = BASE_DIR / "JKNOCrawler"
TR_DIR = BASE_DIR / "TRNOCrwaler"
INFO_DIR = BASE_DIR / "InformationCrawler"
DB_DIR = BASE_DIR / "DBIntegration"

# 경기장 한글-일문 매핑 사전
VENUE_MAP = {
    "도쿄": "東京",
    "나카야마": "中山",
    "한신": "阪神",
    "교토": "京都"
}

def print_menu():
    print("\n" + "=" * 60)
    print("   🐎 넷케이바 (Netkeiba) 기가 크롤링 마스터 파이프라인 🐎")
    print("=" * 60)
    print("크롤링 모드를 선택하세요:")
    print("  1. 과거 경기 결과 수집 (날짜+장소 입력 자동 탐색)")
    print("  2. 토요일 경기 계획 수집 (Automatic Discovery)")
    print("  3. 일요일 경기 계획 수집 (Automatic Discovery)")
    print("  4. 실시간 변경 정보 모니터링 (Information)")
    print("  5. 날씨 및 바장 정보 즉시 수집 (Weather Only)")
    print("  6. 수집된 출마표 CSV 데이터를 DB에 업로드 (MariaDB)")
    print("  7. DB 내 임시 테이블 데이터를 API 테이블로 이관 (JOIN)")
    print("  8. 수집된 [과거 경기 결과] CSV를 DB에 업로드 (DELETE & INSERT)")
    print("  9. [과거 경기 결과] 임시 테이블 데이터를 API 테이블로 이관")
    print("-" * 60)
    print("  [ 협업자 전용 도구 ]")
    print("  10. 경주마 원본 사진 다운로더 (Netkeiba DB 기반 최대 30장)")
    print("=" * 60)
    print("  q. 종료 (Quit)")
    print("=" * 60)


def extract_suffix_from_filename(csv_path: Path, prefix: str) -> str:
    if not csv_path.exists():
        return "unknown"
    stem = csv_path.stem
    if stem.startswith(prefix):
        return stem[len(prefix):]
    return "unknown"

def run_child_crawlers(date_str: str, max_retries: int = 3):
    logger.info(f"\n========== [Phase 3] 하위 디테일 크롤러 연쇄 가동 (날짜/장소: {date_str}) ==========")
    
    for attempt in range(1, max_retries + 1):
        if attempt > 1:
            logger.info(f"\n========== [Phase 3 재시도] 누락 데이터 복구 시도 ({attempt}/{max_retries} 회차) ==========")
            
        any_failed = False
        
        # 1. HRNOCrawler
        logger.info(f"▶ [1/3] HRNOCrawler 가동 중...")
        res = subprocess.run([sys.executable, "main.py", date_str], cwd=HR_DIR)
        if res.returncode == 2: any_failed = True
        
        # 2. JKNOCrawler
        logger.info(f"▶ [2/3] JKNOCrawler 가동 중...")
        res = subprocess.run([sys.executable, "main.py", date_str], cwd=JK_DIR)
        if res.returncode == 2: any_failed = True
        
        # 3. TRNOCrwaler
        logger.info(f"▶ [3/3] TRNOCrwaler 가동 중...")
        res = subprocess.run([sys.executable, "main.py", date_str], cwd=TR_DIR)
        if res.returncode == 2: any_failed = True
                
        if not any_failed:
            logger.info("\n========== [완료] 하위 크롤러 데이터 수집이 성공적으로 마무리되었습니다. ==========")
            return True
        else:
            if attempt < max_retries:
                logger.warning(f"\n[알림] 누락 항목 발생으로 5초 대기 후 {attempt+1}회차 재수집을 시도합니다.")
                time.sleep(5)
            else:
                logger.error(f"\n[경고] {max_retries}회 반복했으나 일부 항목은 수집하지 못했습니다.")
                return False

def run_mode_1_logic(url: str, max_retries: int = 3):
    logger.info(f"\n▶ [Phase 1] 경기 결과 수집 시작: {url}")
    
    success = False
    for attempt in range(1, max_retries + 1):
        if attempt > 1: logger.info(f"--- [Mode 1 재시도] {attempt}/{max_retries} 회차 ---")
        res = subprocess.run([sys.executable, "main.py", url], cwd=WEB_CRAWLER_DIR)
        
        if res.returncode == 0:
            success = True
            break
        elif res.returncode == 3:
            logger.info("👋 사용자 요청으로 전체 작업을 종료합니다.")
            sys.exit(0)
        elif res.returncode == 2:
            logger.warning(f"⚠️ [Phase 1] 일부 데이터 누락 발생. 재시도를 진행합니다.")
        else:
            logger.error(f"❌ [Phase 1] 치명적 오류 발생 (종료코드: {res.returncode})")
            break
            
    logger.info("\n▶ [Phase 2] PK (고유번호) 분배 중...")
    data_dir = WEB_CRAWLER_DIR / "data"
    csv_files = list(data_dir.glob("race_planning_*.csv"))
    if not csv_files:
        logger.error(f"[오류] race_planning CSV를 찾을 수 없습니다.")
        return
        
    latest_csv = max(csv_files, key=lambda p: p.stat().st_mtime)
    subprocess.run([sys.executable, "no_divider_from_race_result.py", latest_csv.name], cwd=WEB_CRAWLER_DIR)
    
    suffix = extract_suffix_from_filename(latest_csv, "race_planning_")
    # run_child_crawlers(suffix)  # 자동화 모드에서는 검증 후 별도 호출

def run_mode_2_logic(url: str, max_retries: int = 3):
    logger.info(f"\n▶ [Phase 1 & 2] 경기 계획 수집 및 PK 분배: {url}")
    
    success = False
    for attempt in range(1, max_retries + 1):
        if attempt > 1: logger.info(f"--- [Mode 2 재시도] {attempt}/{max_retries} 회차 ---")
        res = subprocess.run([sys.executable, "main.py", url], cwd=ENTRY_SHEET_DIR)
        
        if res.returncode == 0:
            success = True
            break
        elif res.returncode == 3:
            logger.info("👋 사용자 요청으로 전체 작업을 종료합니다.")
            sys.exit(0)
        elif res.returncode == 2:
            logger.warning(f"⚠️ [Phase 1] 일부 경주 누락 발생. 재시도를 진행합니다.")
        else:
            logger.error(f"❌ [Phase 1] 치명적 오류 발생 (종료코드: {res.returncode})")
            break

    csv_files = list((ENTRY_SHEET_DIR / "data").glob("api_entry_sheet_2_*.csv"))
    if not csv_files:
        logger.error("[오류] api_entry_sheet_2 CSV를 찾을 수 없습니다.")
        return
        
    latest_csv = max(csv_files, key=lambda p: p.stat().st_mtime)
    suffix = extract_suffix_from_filename(latest_csv, "race_planning_")
    # run_child_crawlers(suffix)  # 자동화 모드에서는 검증 후 별도 호출

def validate_csv_data(date, venue):
    """CSV 파일을 열어 枠(WAKU)와 馬番(CHULNO) 데이터가 모두 존재하는지 검증"""
    csv_path = ENTRY_SHEET_DIR / "data" / f"api_entry_sheet_2_{venue}_{date}.csv"
    if not csv_path.exists():
        logger.warning(f"  [검증 실패] 파일 없음: {csv_path.name}")
        return False
    
    try:
        import csv
        with open(csv_path, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            if not rows:
                return False
            
            for i, row in enumerate(rows, 1):
                waku = row.get('WAKU', '').strip()
                chulno = row.get('CHULNO', '').strip()
                # 枠(WAKU)나 馬番(CHULNO)이 비어있으면 미완성
                if not waku or not chulno:
                    logger.warning(f"  [검증 실패] {csv_path.name} - {i}행 데이터 누락 (WAKU:{waku}, CHULNO:{chulno})")
                    return False
        return True
    except Exception as e:
        logger.error(f"  [검증 에러] {e}")
        return False

def trigger_external_api(date, venue, max_retries=3):
    """외부 AI 예측 시스템 API 호출 (동기 방식)"""
    # 경기장 한글명을 파라미터용으로 변환 (필요시) - 여기서는 일본어 명칭 그대로 사용하거나 매핑 필요
    # API가 일본어 명칭을 받는지 확인 필요 (현재 md에는 {경기장}으로 되어 있음)
    url = f"https://j.mafeel.ai/schedule/deploy/oneRaceDt.do?meet={venue}&raceDt={date}"
    logger.info(f"🚀 외부 API 호출 시작: {url}")
    
    for attempt in range(1, max_retries + 1):
        if attempt > 1:
            logger.info(f"--- [API 호출 재시도] {attempt}/{max_retries} 회차 ---")
            
        try:
            # 동기 방식이므로 응답이 올 때까지 대기. 타임아웃은 넉넉히 10분(600초) 설정
            response = requests.get(url, timeout=600)
            if response.status_code == 200:
                result = response.json()
                if result.get("status") == "OK":
                    logger.info(f"✅ API 호출 성공 (OK): {venue} {date}")
                    return True
                else:
                    logger.error(f"❌ API 응답 오류 (status: {result.get('status')}): {result}")
            else:
                logger.error(f"❌ HTTP 오류 (Status: {response.status_code})")
        except Exception as e:
            logger.error(f"❌ API 호출 중 장애 발생: {e}")
            
        if attempt < max_retries:
            logger.warning("⚠️ 10초 후 API 호출을 재시도합니다.")
            time.sleep(10)
            
    return False

def run_automation_pipeline(mode):
    """완전 자동화 파이프라인 루프 (10분 간격 스캔 -> DB -> API)"""
    day_name = "토요일" if mode == "2" else "일요일" # mode2:토요일, mode3:일요일 
    weekday_target = 5 if mode == "2" else 6 # 5:토요일, 6:일요일 
    
    logger.info(f"\n{'='*60}\n🤖 {day_name} 경기 자동화 파이프라인 가동\n{'='*60}")
    
    while True:
        logger.info(f"\n[시간: {datetime.now().strftime('%H:%M:%S')}] 데이터 스캔 및 수집 시도 중...")
        
        # 1. 대상 탐색
        all_targets = get_all_target_races()
        targets = [t for t in all_targets if datetime.strptime(t['date'], "%Y%m%d").weekday() == weekday_target]
        
        if not targets:
            logger.warning(f"🔍 탐색된 {day_name} 경기가 없습니다. 10분 후 다시 시도합니다.")
            time.sleep(600)
            continue
        
        # 2. 날씨 및 메인 크롤링 실행 (기존 로직 활용)
        process_plan_targets(targets)
        
        # 3. 데이터 완성도 검증
        all_valid = True
        for t in targets:
            if not validate_csv_data(t['date'], t['venue']):
                all_valid = False
                break
        
        if not all_valid:
            logger.info("⚠️ 일부 데이터가 아직 미완성 상태입니다. 10분 후 재시도합니다.")
            time.sleep(600)
            continue
        
        logger.info("✨ 모든 경기 계획 데이터 수집 및 검증 완료! 하위 상세 데이터 수집을 시작합니다.")
        
        # 3.5 하위 디테일 크롤러 가동 (모든 경기장 대상)
        child_success = True
        for t in targets:
            suffix = f"{t['venue']}_{t['date']}"
            if run_child_crawlers(suffix) is False:
                child_success = False

        if child_success:
            send_telegram_message(f"✅ [{day_name}] 모든 데이터를 확실히 CSV로 저장 완료!")
        else:
            send_telegram_message(f"❌ [{day_name}] 일부 CSV 데이터 수집 실패! (DB 적재는 가능한 대상만 진행합니다)")

        logger.info("✨ 모든 데이터 수집 및 검증 완료! 후속 작업을 진행합니다.")
        
        # 4. DB 업로드 (6번) 및 API 이관 (7번) - 경기장별 순차 처리
        # 중복 실행 방지를 위해 유니크한 날짜/경기장 조합 추출
        processed_combos = []
        for t in targets:
            combo = (t['date'], t['venue'])
            if combo not in processed_combos:
                # DB 업로드
                if run_mode_6(t['date'], t['venue']):
                    send_telegram_message(f"✅ [{t['date']} {t['venue']}] CSV를 DB tmp 테이블에 적재 완료!")
                else:
                    send_telegram_message(f"❌ [{t['date']} {t['venue']}] DB tmp 테이블 적재 도중 실패!")
                    continue
                    
                # API 이관
                if run_mode_7(t['date'], t['venue']):
                    send_telegram_message(f"✅ [{t['date']} {t['venue']}] tmp 데이터를 API 테이블에 완벽히 이관 완료!")
                else:
                    send_telegram_message(f"❌ [{t['date']} {t['venue']}] API 테이블 이관 실패!")
                    continue
                    
                # 외부 API 트리거
                if trigger_external_api(t['date'], t['venue']):
                    send_telegram_message(f"🚀 [{t['date']} {t['venue']}] 마지막 단계: API 호출 성공 및 OK 수신!")
                else:
                    send_telegram_message(f"⚠️ [{t['date']} {t['venue']}] 마지막 단계: API 호출 실패 또는 에러 발생!")
                    
                processed_combos.append(combo)
        
        logger.info(f"\n✅ {day_name} 모든 자동화 작업이 성공적으로 종료되었습니다. 프로그램을 종료합니다.")
        sys.exit(0)

def run_mode_1():
    """1번 모드: 날짜와 경기장 한글명을 입력받아 자동 탐색 후 수집"""
    print("\n" + "┌" + "─" * 45 + "┐")
    print("│         [ 수집 대상 경기 정보 입력 ]         │")
    print("├" + "─" * 45 + "┤")
    print("│  1. 날짜 입력 (형식: YYYYMMDD 예: 20260419)  │")
    print("│  2. 경기장 이름 (도쿄, 나카야마, 한신, 교토) │")
    print("└" + "─" * 45 + "┘")
    
    date_input = input("\n📅 수집할 날짜를 입력하세요: ").strip()
    if not re.match(r"^\d{8}$", date_input):
        print("❌ 오류: 날짜 형식이 올바르지 않습니다. (8자리 숫자 필요)")
        return

    venue_input = input("🏟️ 경기장 이름을 입력하세요 (도쿄/나카야마/한신/교토): ").strip()
    if venue_input not in VENUE_MAP:
        print(f"❌ 오류: '{venue_input}'은(는) 지원하지 않는 경기장입니다.")
        return

    japanese_venue = VENUE_MAP[venue_input]
    logger.info(f"🔍 자동 탐색 시작: {date_input} {venue_input}({japanese_venue})")

    # discovery.py 기능을 활용해 1R URL 확보
    found_races = discover_races(date_input)
    target_race = next((r for r in found_races if r['venue'] == japanese_venue), None)

    if not target_race:
        print(f"⚠️ 결과: 해당 날짜({date_input})에 {venue_input} 경기가 열리지 않았거나 찾을 수 없습니다.")
        return

    logger.info(f"🎯 탐색 성공! 1경기 URL: {target_race['url']}")
    
    # 결과 수집 프로세스 가동 (Result Mode)
    run_mode_1_logic(target_race['url'])

def run_mode_2():
    """2번 모드: 이번 주 토요일 경기 자동 탐색 및 계획 수집"""
    logger.info("\n========== [자동 탐색] 토요일 경기 계획(Shutuba) 수집 시작 ==========")
    all_targets = get_all_target_races()
    # 요일 필터링 (5: 토요일)
    targets = [t for t in all_targets if datetime.strptime(t['date'], "%Y%m%d").weekday() == 5]
    
    if not targets:
        logger.warning("🔍 검색된 토요일 경기 계획이 없습니다.")
        return

    process_plan_targets(targets)

def run_mode_3():
    """3번 모드: 이번 주 일요일 경기 자동 탐색 및 계획 수집"""
    logger.info("\n========== [자동 탐색] 일요일 경기 계획(Shutuba) 수집 시작 ==========")
    all_targets = get_all_target_races()
    # 요일 필터링 (6: 일요일)
    targets = [t for t in all_targets if datetime.strptime(t['date'], "%Y%m%d").weekday() == 6]
    
    if not targets:
        logger.warning("🔍 검색된 일요일 경기 계획이 없습니다.")
        return

    process_plan_targets(targets)

def process_plan_targets(targets):
    """찾은 타겟들에 대해 날씨 수집 및 메인 크롤링 실행"""
    print_discovery_results(targets)
    
    dates = set(t['date'] for t in targets)
    for d in dates:
        logger.info(f"\n▶ 해당일({d}) 날씨 및 마장 상태 예보 수집 중...")
        run_weather_crawl(d)
        
    for t in targets:
        run_mode_2_logic(t['url'])

def run_mode_4():
    logger.info("\n========== 실시간 변경 정보 모니터링 (Information) 가동 ==========")
    if not (INFO_DIR / "main.py").exists():
        logger.warning(f"[경고] {INFO_DIR}/main.py 파일을 찾을 수 없습니다.")
    else:
        subprocess.run([sys.executable, "main.py"], cwd=INFO_DIR)

def run_mode_5():
    """5번 모드: 날씨 및 바장 정보 단독 수집 (자동 탐색 기반)"""
    logger.info("\n========== 날씨 및 바장 정보 단독 수집 시작 (전용 모드) ==========")
    targets = get_all_target_races()
    dates = set(t['date'] for t in targets)
    
    if not dates:
        dates.add(datetime.now().strftime("%Y%m%d"))
    
    for d in dates:
        run_weather_crawl(d)
    logger.info("========== 날씨 수집 작업이 종료되었습니다. ==========")

def run_mode_6(date=None, venue=None, max_retries=3):
    """6번 모드: CSV 데이터를 MariaDB에 업로드"""
    logger.info("\n========== [Phase 4] 수집된 CSV 데이터 DB 업로드 시작 ==========")
    if not (DB_DIR / "mariadb_upsert.py").exists():
        logger.error(f"[오류] {DB_DIR}/mariadb_upsert.py 파일을 찾을 수 없습니다.")
        return False
        
    cmd = [sys.executable, "mariadb_upsert.py"]
    if date and venue:
        cmd.extend(["--date", date, "--venue", venue])
        
    for attempt in range(1, max_retries + 1):
        if attempt > 1:
            logger.info(f"--- [DB 업로드 재시도] {attempt}/{max_retries} 회차 ---")
            
        res = subprocess.run(cmd, cwd=DB_DIR)
        if res.returncode == 0:
            logger.info("\n========== DB 업로드 작업이 완료되었습니다. ==========")
            return True
        else:
            logger.warning(f"⚠️ DB 업로드 실패 (종료코드: {res.returncode})")
            
        if attempt < max_retries:
            logger.warning("⚠️ 5초 후 DB 업로드를 재시도합니다.")
            time.sleep(5)
            
    return False

def run_mode_7(date=None, venue=None, max_retries=3):
    """7번 모드: tmp 테이블 데이터를 api 테이블로 이관 (JOIN)"""
    logger.info("\n========== [Phase 5] DB API 테이블 데이터 이관 시작 ==========")
    if not (DB_DIR / "mariadb_api_transfer.py").exists():
        logger.error(f"[오류] {DB_DIR}/mariadb_api_transfer.py 파일을 찾을 수 없습니다.")
        return False
        
    cmd = [sys.executable, "mariadb_api_transfer.py"]
    if date and venue:
        cmd.extend(["--date", date, "--venue", venue])
        
    for attempt in range(1, max_retries + 1):
        if attempt > 1:
            logger.info(f"--- [API 이관 재시도] {attempt}/{max_retries} 회차 ---")
            
        res = subprocess.run(cmd, cwd=DB_DIR)
        if res.returncode == 0:
            logger.info("\n========== 데이터 이관 작업이 완료되었습니다. ==========")
            return True
        else:
            logger.warning(f"⚠️ API 테이블 이관 실패 (종료코드: {res.returncode})")
            
        if attempt < max_retries:
            logger.warning("⚠️ 5초 후 API 테이블 이관을 재시도합니다.")
            time.sleep(5)
            
    return False

def run_mode_8():
    """8번 모드: 과거 경기 결과 CSV 데이터를 DB에 업로드 (DELETE & INSERT)"""
    print("\n" + "┌" + "─" * 45 + "┐")
    print("│       [ 과거 경기 결과 DB 업로드 대상 입력 ]       │")
    print("├" + "─" * 45 + "┤")
    print("│  1. 날짜 입력 (형식: YYYYMMDD 예: 20260419)  │")
    print("│  2. 경기장 이름 (도쿄, 나카야마, 한신, 교토) │")
    print("└" + "─" * 45 + "┘")
    
    date_input = input("\n📅 수집된 날짜를 입력하세요: ").strip()
    if not re.match(r"^\d{8}$", date_input):
        print("❌ 오류: 날짜 형식이 올바르지 않습니다.")
        return
    venue_input = input("🏟️ 경기장 이름을 입력하세요 (도쿄/나카야마/한신/교토): ").strip()
    if venue_input not in VENUE_MAP:
        print(f"❌ 오류: '{venue_input}'은(는) 지원하지 않는 경기장입니다.")
        return
        
    japanese_venue = VENUE_MAP[venue_input]
    logger.info(f"\n========== [Phase 4-Result] 과거 경기 결과 CSV DB 업로드 시작: {date_input} {japanese_venue} ==========")
    if not (DB_DIR / "mariadb_result_upsert.py").exists():
        logger.error(f"[오류] mariadb_result_upsert.py 파일을 찾을 수 없습니다.")
    else:
        subprocess.run([sys.executable, "mariadb_result_upsert.py", "--date", date_input, "--venue", japanese_venue], cwd=DB_DIR)
        logger.info("\n========== DB 업로드 작업이 완료되었습니다. ==========")

def run_mode_9():
    """9번 모드: 과거 경기 결과 임시 테이블 데이터를 API 테이블로 이관"""
    print("\n" + "┌" + "─" * 45 + "┐")
    print("│       [ 과거 경기 결과 API 이관 대상 입력 ]        │")
    print("├" + "─" * 45 + "┤")
    print("│  1. 날짜 입력 (형식: YYYYMMDD 예: 20260419)  │")
    print("│  2. 경기장 이름 (도쿄, 나카야마, 한신, 교토) │")
    print("└" + "─" * 45 + "┘")
    
    date_input = input("\n📅 이관할 날짜를 입력하세요: ").strip()
    if not re.match(r"^\d{8}$", date_input):
        print("❌ 오류: 날짜 형식이 올바르지 않습니다.")
        return
    venue_input = input("🏟️ 경기장 이름을 입력하세요 (도쿄/나카야마/한신/교토): ").strip()
    if venue_input not in VENUE_MAP:
        print(f"❌ 오류: '{venue_input}'은(는) 지원하지 않는 경기장입니다.")
        return
        
    japanese_venue = VENUE_MAP[venue_input]
    logger.info(f"\n========== [Phase 5-Result] 과거 경기 결과 API 이관 시작: {date_input} {japanese_venue} ==========")
    if not (DB_DIR / "mariadb_result_api_transfer.py").exists():
        logger.error(f"[오류] mariadb_result_api_transfer.py 파일을 찾을 수 없습니다.")
    else:
        subprocess.run([sys.executable, "mariadb_result_api_transfer.py", "--date", date_input, "--venue", japanese_venue], cwd=DB_DIR)
        logger.info("\n========== 데이터 이관 작업이 완료되었습니다. ==========")

def run_mode_10():
    """10번 모드: 협업자용 - 말 원본 사진 다운로더"""
    print("\n" + "┌" + "─" * 45 + "┐")
    print("│         [ 협업자용: 사진 다운로더 ]          │")
    print("├" + "─" * 45 + "┤")
    print("│  1. 날짜 입력 (형식: YYYYMMDD 예: 20260419)  │")
    print("│  2. 경기장 이름 (도쿄, 나카야마, 한신, 교토) │")
    print("└" + "─" * 45 + "┘")
    
    date_input = input("\n📅 수집할 날짜를 입력하세요: ").strip()
    if not re.match(r"^\d{8}$", date_input):
        print("❌ 오류: 날짜 형식이 올바르지 않습니다.")
        return
    venue_input = input("🏟️ 경기장 이름을 입력하세요 (도쿄/나카야마/한신/교토): ").strip()
    if venue_input not in VENUE_MAP:
        print(f"❌ 오류: '{venue_input}'은(는) 지원하지 않는 경기장입니다.")
        return
        
    japanese_venue = VENUE_MAP[venue_input]
    date_suffix = f"{japanese_venue}_{date_input}"
    csv_filename = f"HRNO_{date_suffix}_list.csv"
    csv_path = HR_DIR / "nodata" / csv_filename
    
    if not csv_path.exists():
        print(f"❌ 오류: 해당 날짜/경기장의 CSV 파일을 찾을 수 없습니다: {csv_path}")
        return
        
    logger.info(f"\n========== [협업자 도구] 경주마 원본 사진 다운로드 시작: {date_input} {japanese_venue} ==========")
    if not (HR_DIR / "image_downloader.py").exists():
        logger.error(f"[오류] HRNOCrawler/image_downloader.py 파일을 찾을 수 없습니다.")
    else:
        subprocess.run([sys.executable, "image_downloader.py", "--csv", str(csv_path)], cwd=HR_DIR)
        logger.info("\n========== 다운로드 작업이 완료되었습니다. ==========")

def print_discovery_results(targets):
    print("\n" + "-" * 50)
    print(f"📡 자동 탐색 성공! 총 {len(targets)}개의 대상을 찾았습니다.")
    print("-" * 50)
    for i, t in enumerate(targets, 1):
        print(f" {i}. [{t['date']}] {t['venue']} | {t['url']}")
    print("-" * 50 + "\n")

def main():
    import argparse
    parser = argparse.ArgumentParser(description="넷케이바 자동화 마스터 파이프라인")
    parser.add_argument("--auto", choices=["2", "3"], help="자동화 모드 실행 (2:토요일, 3:일요일)")
    args = parser.parse_args()

    # 0. 자동화 모드 체크
    if args.auto:
        run_automation_pipeline(args.auto)
        return

    # 0. 넷케이바 프리미엄 세션 자동 확인
    print("\n" + "="*60)
    print("🔑 넷케이바 프리미엄 세션 자동 확인 중...")
    try:
        get_netkeiba_cookies()
        print("✅ 세션 준비 완료!")
    except Exception as e:
        print(f"⚠️ 자동 로그인 실패: {e}")
        print("💡 일반 모드로 진행하거나 설정을 확인해 주세요.")
    print("="*60 + "\n")

    while True:
        print_menu()
        mode = input("선택하실 번호를 입력하세요 (1~9 또는 q): ").strip().lower()
        
        if mode == 'q':
            print("\n프로그램을 종료합니다.")
            break
            
        if mode not in ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10"]:
            print("\n[오류] 잘못된 입력입니다.")
            continue

        try:
            if mode == "1":
                run_mode_1()
            elif mode == "2":
                run_mode_2()
            elif mode == "3":
                run_mode_3()
            elif mode == "4":
                run_mode_4()
            elif mode == "5":
                run_mode_5()
            elif mode == "6":
                print("\n" + "┌" + "─" * 45 + "┐")
                print("│        [ 출마표 DB 업로드 대상 입력 ]        │")
                print("├" + "─" * 45 + "┤")
                print("│  * 미입력 시 자동 탐색된 전체 대상 처리      │")
                print("│  1. 날짜 (YYYYMMDD) / 2. 경기장 한글명       │")
                print("└" + "─" * 45 + "┘")
                d_in = input("📅 날짜 (엔터: 전체): ").strip()
                v_in = input("🏟️ 경기장 (엔터: 전체): ").strip()
                japanese_v = VENUE_MAP.get(v_in) if v_in else None
                run_mode_6(d_in if d_in else None, japanese_v)
            elif mode == "7":
                print("\n" + "┌" + "─" * 45 + "┐")
                print("│        [ DB API 테이블 이관 대상 입력 ]       │")
                print("├" + "─" * 45 + "┤")
                print("│  * 미입력 시 자동 탐색된 전체 대상 처리      │")
                print("│  1. 날짜 (YYYYMMDD) / 2. 경기장 한글명       │")
                print("└" + "─" * 45 + "┘")
                d_in = input("📅 날짜 (엔터: 전체): ").strip()
                v_in = input("🏟️ 경기장 (엔터: 전체): ").strip()
                japanese_v = VENUE_MAP.get(v_in) if v_in else None
                run_mode_7(d_in if d_in else None, japanese_v)
            elif mode == "8":
                run_mode_8()
            elif mode == "9":
                run_mode_9()
            elif mode == "10":
                run_mode_10()
            
            logger.info("\n[성공] 파이프라인 작업이 종료되었습니다.")
        except KeyboardInterrupt:
            logger.warning("\n[중단] 작업이 중지되었습니다.")
        except Exception as e:
            logger.error(f"\n[장애] 예기치 못한 오류: {e}")
        
        print("\n메뉴로 돌아가려면 엔터를 누르세요...")
        input()

if __name__ == "__main__":
    main()
