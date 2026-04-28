import os
import sys
import time
import subprocess
import logging
import re
from datetime import datetime
from pathlib import Path

# Paths Setup
BASE_DIR = Path(__file__).resolve().parent

# discovery 및 weather 모듈을 시스템 경로에 추가
sys.path.append(str(BASE_DIR))
from WebCrawler.discovery import discover_races, get_all_target_races
from WeatherCrawler.main import run_weather_crawl
from netkeiba_auth import get_netkeiba_cookies

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
    print("  2. 이번 주 경기 계획 수집 (Automatic Discovery)")
    print("  3. 실시간 변경 정보 모니터링 (Information)")
    print("  4. 날씨 및 바장 정보 즉시 수집 (Weather Only)")
    print("  5. 수집된 출마표 CSV 데이터를 DB에 업로드 (MariaDB)")
    print("  6. DB 내 임시 테이블 데이터를 API 테이블로 이관 (JOIN)")
    print("  7. 수집된 [과거 경기 결과] CSV를 DB에 업로드 (DELETE & INSERT)")
    print("  8. [과거 경기 결과] 임시 테이블 데이터를 API 테이블로 이관")
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
            break
        else:
            if attempt < max_retries:
                logger.warning(f"\n[알림] 누락 항목 발생으로 5초 대기 후 {attempt+1}회차 재수집을 시도합니다.")
                time.sleep(5)
            else:
                logger.error(f"\n[경고] {max_retries}회 반복했으나 일부 항목은 수집하지 못했습니다.")

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
    run_child_crawlers(suffix)

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
    suffix = extract_suffix_from_filename(latest_csv, "api_entry_sheet_2_")
    run_child_crawlers(suffix)

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
    """2번 모드: 이번 주말 경기 자동 탐색 및 계획 수집"""
    logger.info("\n========== [자동 탐색] 이번 주 경기 계획(Shutuba) 수집 시작 ==========")
    targets = get_all_target_races()
    if not targets:
        logger.warning("🔍 검색된 경기 계획이 없습니다. (아직 공시 전이거나 대상지가 아님)")
        return

    print_discovery_results(targets)
    
    # 경기 계획이 있는 날짜들을 추출해 날씨(예측) 정보를 선행 수집
    dates = set(t['date'] for t in targets)
    for d in dates:
        logger.info(f"\n▶ 해당일({d}) 날씨 및 마장 상태 예보 수집 중...")
        run_weather_crawl(d)
        
    for t in targets:
        run_mode_2_logic(t['url'])

def run_mode_3():
    logger.info("\n========== 실시간 변경 정보 모니터링 (Information) 가동 ==========")
    if not (INFO_DIR / "main.py").exists():
        logger.warning(f"[경고] {INFO_DIR}/main.py 파일을 찾을 수 없습니다.")
    else:
        subprocess.run([sys.executable, "main.py"], cwd=INFO_DIR)

def run_mode_4():
    """4번 모드: 날씨 및 바장 정보 단독 수집 (자동 탐색 기반)"""
    logger.info("\n========== 날씨 및 바장 정보 단독 수집 시작 (전용 모드) ==========")
    targets = get_all_target_races()
    dates = set(t['date'] for t in targets)
    
    if not dates:
        dates.add(datetime.now().strftime("%Y%m%d"))
    
    for d in dates:
        run_weather_crawl(d)
    logger.info("========== 날씨 수집 작업이 종료되었습니다. ==========")

def run_mode_5():
    """5번 모드: CSV 데이터를 MariaDB에 업로드"""
    logger.info("\n========== [Phase 4] 수집된 CSV 데이터 DB 업로드 시작 ==========")
    if not (DB_DIR / "mariadb_upsert.py").exists():
        logger.error(f"[오류] {DB_DIR}/mariadb_upsert.py 파일을 찾을 수 없습니다.")
    else:
        subprocess.run([sys.executable, "mariadb_upsert.py"], cwd=DB_DIR)
        logger.info("\n========== DB 업로드 작업이 완료되었습니다. ==========")

def run_mode_6():
    """6번 모드: tmp 테이블 데이터를 api 테이블로 이관 (JOIN)"""
    logger.info("\n========== [Phase 5] DB API 테이블 데이터 이관 시작 ==========")
    if not (DB_DIR / "mariadb_api_transfer.py").exists():
        logger.error(f"[오류] {DB_DIR}/mariadb_api_transfer.py 파일을 찾을 수 없습니다.")
    else:
        subprocess.run([sys.executable, "mariadb_api_transfer.py"], cwd=DB_DIR)
        logger.info("\n========== 데이터 이관 작업이 완료되었습니다. ==========")

def run_mode_7():
    """7번 모드: 과거 경기 결과 CSV 데이터를 DB에 업로드 (DELETE & INSERT)"""
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

def run_mode_8():
    """8번 모드: 과거 경기 결과 임시 테이블 데이터를 API 테이블로 이관"""
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

def print_discovery_results(targets):
    print("\n" + "-" * 50)
    print(f"📡 자동 탐색 성공! 총 {len(targets)}개의 대상을 찾았습니다.")
    print("-" * 50)
    for i, t in enumerate(targets, 1):
        print(f" {i}. [{t['date']}] {t['venue']} | {t['url']}")
    print("-" * 50 + "\n")

def main():
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
        mode = input("선택하실 번호를 입력하세요 (1, 2, 3, 4, 5, 6, 7, 8 또는 q): ").strip().lower()
        
        if mode == 'q':
            print("\n프로그램을 종료합니다.")
            break
            
        if mode not in ["1", "2", "3", "4", "5", "6", "7", "8"]:
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
                run_mode_6()
            elif mode == "7":
                run_mode_7()
            elif mode == "8":
                run_mode_8()
            
            logger.info("\n[성공] 파이프라인 작업이 종료되었습니다.")
        except KeyboardInterrupt:
            logger.warning("\n[중단] 작업이 중지되었습니다.")
        except Exception as e:
            logger.error(f"\n[장애] 예기치 못한 오류: {e}")
        
        print("\n메뉴로 돌아가려면 엔터를 누르세요...")
        input()

if __name__ == "__main__":
    main()
