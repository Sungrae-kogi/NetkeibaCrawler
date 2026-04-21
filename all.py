import os
import sys
import time
import subprocess
import logging
from datetime import datetime
from pathlib import Path

# Paths Setup
BASE_DIR = Path(__file__).resolve().parent
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


def print_menu():
    print("=" * 60)
    print("   🐎 넷케이바 (Netkeiba) 기가 크롤링 마스터 파이프라인 🐎")
    print("=" * 60)
    print("크롤링 모드를 선택하세요:")
    print("  1. 과거 경기 결과 수집 모드 (result)")
    print("  2. 미래 경기 계획 수집 모드 (shutuba/entry_sheet)")
    print("  3. 실시간 변경 정보 모니터링 (Information)")
    print("=" * 60)


def extract_suffix_from_filename(csv_path: Path, prefix: str) -> str:
    if not csv_path.exists():
        return "unknown"
    stem = csv_path.stem
    if stem.startswith(prefix):
        return stem[len(prefix):]
    return "unknown"

def run_child_crawlers(date_str: str, max_retries: int = 3):
    logger.info(f"\n========== [Phase 3] 하위 디테일 크롤러 연쇄 가동 (Auto-Healing 모드 시작, 날짜/장소: {date_str}) ==========")
    
    for attempt in range(1, max_retries + 1):
        if attempt > 1:
            logger.info(f"\n========== [Phase 3 재시도] 누락 데이터 복구 시도 ({attempt}/{max_retries} 회차) ==========")
            
        any_failed = False
        
        # 1. HRNOCrawler
        logger.info(f"▶ [1/3] HRNOCrawler (말 상세 프로필) 가동 중...")
        if not (HR_DIR / "main.py").exists():
            logger.warning(f"[경고] {HR_DIR}/main.py 파일을 찾을 수 없어 건너뜁니다.")
        else:
            res = subprocess.run([sys.executable, "main.py", date_str], cwd=HR_DIR)
            if res.returncode == 2:
                any_failed = True
            elif res.returncode != 0:
                logger.error(f"[CRITICAL] HRNOCrawler가 예기치 않게 종료되었습니다 (코드: {res.returncode})")
                raise Exception("HRNOCrawler Crash")
        
        # 2. JKNOCrawler
        logger.info(f"▶ [2/3] JKNOCrawler (기수 상세 프로필) 가동 중...")
        if not (JK_DIR / "main.py").exists():
            logger.warning(f"[경고] {JK_DIR}/main.py 파일을 찾을 수 없어 건너뜁니다.")
        else:
            res = subprocess.run([sys.executable, "main.py", date_str], cwd=JK_DIR)
            if res.returncode == 2:
                any_failed = True
            elif res.returncode != 0:
                logger.error(f"[CRITICAL] JKNOCrawler가 예기치 않게 종료되었습니다 (코드: {res.returncode})")
                raise Exception("JKNOCrawler Crash")
        
        # 3. TRNOCrwaler
        logger.info(f"▶ [3/3] TRNOCrwaler (조교사 상세 프로필) 가동 중...")
        if not (TR_DIR / "main.py").exists():
            logger.warning(f"[경고] {TR_DIR}/main.py 파일을 찾을 수 없어 건너뜁니다.")
        else:
            res = subprocess.run([sys.executable, "main.py", date_str], cwd=TR_DIR)
            if res.returncode == 2:
                any_failed = True
            elif res.returncode != 0:
                logger.error(f"[CRITICAL] TRNOCrwaler가 예기치 않게 종료되었습니다 (코드: {res.returncode})")
                raise Exception("TRNOCrwaler Crash")
                
        if not any_failed:
            logger.info("\n========== [완료] 하위 디테일 크롤러들의 모든 수집이 누락 없이 완료되었습니다! ==========")
            break
        else:
            if attempt < max_retries:
                logger.warning(f"\n[알림] 위 크롤러 중 누락된 항목이 발생하여 5초 대기 후 {attempt+1}회차 핀셋 재수집을 시도합니다.")
                time.sleep(5)
            else:
                logger.error(f"\n[경고] 최선을 다해 {max_retries}회 반복했으나 일부 항목은 끝내 수집하지 못했습니다. 추후 별도 점검이 필요합니다.")
                
    logger.info("========== 전체 마스터 파이프라인 데이터 분배 완료! ==========")

def run_mode_1(url: str):
    logger.info("\n========== [Phase 1] 경기 결과 웹 크롤링 수집 시작 ==========")
    subprocess.run([sys.executable, "main.py", url], cwd=WEB_CRAWLER_DIR)
    
    logger.info("\n========== [Phase 2] PK (고유번호) 분배 및 저장 중 ==========")
    data_dir = WEB_CRAWLER_DIR / "data"
    
    csv_files = list(data_dir.glob("race_planning_*.csv"))
    if not csv_files:
        logger.error(f"[오류] 경로({data_dir}) 내에서 race_planning CSV 파일을 찾을 수 없습니다.")
        return
        
    latest_csv = max(csv_files, key=lambda p: p.stat().st_mtime)
    subprocess.run([sys.executable, "no_divider_from_race_result.py", latest_csv.name], cwd=WEB_CRAWLER_DIR)
    
    suffix = extract_suffix_from_filename(latest_csv, "race_planning_")
    run_child_crawlers(suffix)

def run_mode_2(url: str):
    logger.info("\n========== [Phase 1 & 2] 경기 계획 수집 및 자동 PK 분배 시작 ==========")
    subprocess.run([sys.executable, "main.py", url], cwd=ENTRY_SHEET_DIR)
    
    csv_files = list((ENTRY_SHEET_DIR / "data").glob("api_entry_sheet_2_*.csv"))
    if not csv_files:
        logger.error("[오류] api_entry_sheet_2 CSV 파일을 찾을 수 없습니다.")
        return
        
    latest_csv = max(csv_files, key=lambda p: p.stat().st_mtime)
    suffix = extract_suffix_from_filename(latest_csv, "api_entry_sheet_2_")
    run_child_crawlers(suffix)

def run_mode_3():
    logger.info("\n========== 실시간 변경 정보 모니터링 (Information) 가동 ==========")
    if not (INFO_DIR / "main.py").exists():
        logger.warning(f"[경고] {INFO_DIR}/main.py 파일을 찾을 수 없습니다.")
    else:
        subprocess.run([sys.executable, "main.py"], cwd=INFO_DIR)

def main():
    while True:
        print_menu()
        print("  q. 종료 (Quit)")
        mode = input("선택하실 번호를 입력하세요 (1, 2, 3 또는 q): ").strip().lower()
        
        if mode == 'q':
            print("\n프로그램을 종료합니다.")
            break
            
        if mode not in ["1", "2", "3"]:
            print("\n[오류] 잘못된 입력입니다. 다시 입력해주세요.")
            continue
            
        # 1, 2번 모드일 때만 URL 입력을 받음
        url = ""
        
        if mode in ["1", "2"]:
            print("\n[안내] 자동으로 접근하는 것을 막고 보안을 뚫기 위해, 수동 URL 복사가 필요합니다.")
            url = input("시작하실 1경기 웹페이지의 URL 주소를 통째로 복사해서 붙여넣고 엔터를 치세요 (뒤로 가려면 'q' 입력):\n> ").strip()
            
            if url.lower() == 'q':
                continue
                
            if not url:
                print("\n[오류] URL이 입력되지 않았습니다.")
                continue

        try:
            if mode == "1":
                run_mode_1(url)
            elif mode == "2":
                run_mode_2(url)
            elif mode == "3":
                run_mode_3()
            
            logger.info("\n[성공] 요청하신 파이프라인 작업이 정상적으로 종료되었습니다.")
        except KeyboardInterrupt:
            logger.warning("\n[중단] 사용자에 의해 작업이 중지되었습니다. 메인 메뉴로 돌아갑니다.")
        except Exception as e:
            logger.error(f"\n[장애] 작업 중 예상치 못한 오류가 발생했습니다: {e}")
            logger.info("상세 로그를 확인하거나 네트워크 상태를 점검해 주세요.")
        
        print("\n" + "-" * 60)
        print("메뉴로 돌아가려면 엔터를 누르세요...")
        input()

if __name__ == "__main__":
    main()
