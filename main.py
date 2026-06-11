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
sys.path.append(str(BASE_DIR / "src"))
from src.crawlers.WebCrawler.discovery import discover_races, get_all_target_races
from src.crawlers.WeatherCrawler.main import run_weather_crawl
from netkeiba_auth import get_netkeiba_cookies
from src.reporting.Reporting.email_report import run_reporting_pipeline

def send_telegram_message(message: str):
    """텔레그램 봇으로 메시지를 전송합니다."""
    config_path = BASE_DIR / "config" / "config.json"
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

class Tee(object):
    def __init__(self, *files):
        self.files = files
    def write(self, obj):
        for f in self.files:
            try:
                f.write(obj)
                f.flush()
            except:
                pass
    def flush(self):
        for f in self.files:
            try:
                f.flush()
            except:
                pass

# 로그 파일 오픈 (a 모드)
master_f = open(LOG_FILE, 'a', encoding='utf-8')

# 표준 출력/에러 리다이렉션
original_stdout = sys.stdout
sys.stdout = Tee(sys.stdout, master_f)
sys.stderr = Tee(sys.stderr, master_f)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout) # sys.stdout(Tee로 감싸짐)으로 보내서 콘솔과 파일 모두에 기록되도록 수정
    ]
)
logger = logging.getLogger("Master")

import msvcrt

LOCK_FILE_PATH = BASE_DIR / "app.lock"
lock_file_handle = None

def acquire_app_lock():
    global lock_file_handle
    try:
        lock_file_handle = open(LOCK_FILE_PATH, 'w')
        # Acquire lock on the file
        msvcrt.locking(lock_file_handle.fileno(), msvcrt.LK_NBLCK, 1)
        logger.info("🔒 프로그램 실행 독점 권한(Lock)을 획득했습니다.")
    except (IOError, OSError):
        logger.error("⚠️ 이미 다른 Netkeiba Crawler 인스턴스가 실행 중입니다. 안전을 위해 프로그램을 즉시 종료합니다.")
        sys.exit(0)

# 실행 즉시 중복 실행 방지 락 획득 시도
acquire_app_lock()

WEB_CRAWLER_DIR = BASE_DIR / "src" / "crawlers" / "WebCrawler"
ENTRY_SHEET_DIR = WEB_CRAWLER_DIR / "entry_sheet_2"
HR_DIR = BASE_DIR / "src" / "crawlers" / "HRNOCrawler"
JK_DIR = BASE_DIR / "src" / "crawlers" / "JKNOCrawler"
TR_DIR = BASE_DIR / "src" / "crawlers" / "TRNOCrwaler"
INFO_DIR = BASE_DIR / "src" / "crawlers" / "InformationCrawler"
DB_DIR = BASE_DIR / "src" / "database" / "DBIntegration"

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
    print(" [ 운영 가이드 ]")
    print(" 💡 매주 금요일 18:00: 지난 주 경기 결과의 구간별 기록 업데이트")
    print("    -> 1번(수집) -> 8번(DB업로드) -> 9번(API이관) 순서로 수행")
    print("-" * 60)
    print(" [ 자동화 인수 (--auto) 설명 ]")
    print(" --auto 2 : 토요일 계획 자동화 (2 -> 6 -> 7 -> AI API 호출)")
    print(" --auto 3 : 일요일 계획 자동화 (3 -> 6 -> 7 -> AI API 호출)")
    print(" --auto 4 : 지난 토요일 결과 자동화 (1 -> 8 -> 9 수행 + 결과 리포트 메일 발송)")
    print(" --auto 5 : 지난 일요일 결과 자동화 (1 -> 8 -> 9 수행 + 결과 리포트 메일 발송)")
    print(" --auto 6 : 지난 주 구간별 기록 자동 업데이트 (금요일 18:00 권장)")
    print(" --auto 7 : 한국 결과 리포트 자동 생성 및 발송")
    print("=" * 60)
    print("크롤링 모드를 선택하세요:")
    print("  1. 과거 경기 결과 수집 (날짜+장소 입력 자동 탐색)")
    print("  2. 이번주 토요일 경기 계획 수집 (Automatic Discovery)")
    print("  3. 이번주 일요일 경기 계획 수집 (Automatic Discovery)")
    print("  4. 실시간 변경 정보 모니터링 (Information)")
    print("  5. 날씨 및 바장 정보 즉시 수집 (Weather Only)")
    print("  6. [계획] 수집된 출마표 CSV를 DB에 업로드 (MariaDB)")
    print("  7. [계획] DB 임시 테이블 데이터를 API 테이블로 이관")
    print("  8. [결과] 수집된 결과 CSV를 DB에 업로드 (DELETE & INSERT)")
    print("  9. [결과] DB 임시 테이블 데이터를 API 테이블로 이관")
    print("-" * 60)
    print("  [ 협업자 전용 도구 ]")
    print("  10. 지난 주 구간별 기록 업데이트 (Lap Time Update)")
    print("  11. [협업자용] 경주마 원본 사진 다운로더")
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

def run_subprocess_with_logging(cmd, cwd, env=None):
    """자식 프로세스의 출력을 실시간으로 읽어와 부모의 sys.stdout 스트림에 전달합니다."""
    import subprocess
    try:
        process = subprocess.Popen(
            cmd,
            cwd=cwd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding='utf-8',
            errors='replace',
            env=env
        )
    except Exception as e:
        logger.error(f"프로세스 가동 실패: {cmd} / {e}")
        return subprocess.CompletedProcess(cmd, 1, stdout="", stderr=str(e))

    stdout_lines = []
    while True:
        line = process.stdout.readline()
        if not line and process.poll() is not None:
            break
        if line:
            sys.stdout.write(line)
            sys.stdout.flush()
            stdout_lines.append(line)

    returncode = process.poll()
    return subprocess.CompletedProcess(cmd, returncode, stdout="".join(stdout_lines))


def run_child_crawlers(date_str: str, max_retries: int = 3):
    logger.info(f"\n========== [Phase 3] 하위 디테일 크롤러 연쇄 가동 (날짜/장소: {date_str}) ==========")
    
    for attempt in range(1, max_retries + 1):
        if attempt > 1:
            logger.info(f"\n========== [Phase 3 재시도] 누락 데이터 복구 시도 ({attempt}/{max_retries} 회차) ==========")
            
        any_failed = False
        
        # 1. HRNOCrawler
        logger.info(f"▶ [1/3] HRNOCrawler 가동 중...")
        res = run_subprocess_with_logging([sys.executable, "main.py", date_str], cwd=HR_DIR)
        if res.returncode == 2: any_failed = True
        
        # 2. JKNOCrawler
        logger.info(f"▶ [2/3] JKNOCrawler 가동 중...")
        res = run_subprocess_with_logging([sys.executable, "main.py", date_str], cwd=JK_DIR)
        if res.returncode == 2: any_failed = True
        
        # 3. TRNOCrwaler
        logger.info(f"▶ [3/3] TRNOCrwaler 가동 중...")
        res = run_subprocess_with_logging([sys.executable, "main.py", date_str], cwd=TR_DIR)
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
        res = run_subprocess_with_logging([sys.executable, "main.py", url], cwd=WEB_CRAWLER_DIR)
        
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
    run_subprocess_with_logging([sys.executable, "no_divider_from_race_result.py", latest_csv.name], cwd=WEB_CRAWLER_DIR)
    
    suffix = extract_suffix_from_filename(latest_csv, "race_planning_")
    # run_child_crawlers(suffix)  # 자동화 모드에서는 검증 후 별도 호출

def run_mode_2_logic(url: str, max_retries: int = 3):
    logger.info(f"\n▶ [Phase 1 & 2] 경기 계획 수집 및 PK 분배: {url}")
    
    """
    2026.06.09
    Mode 2 로직은 경주 계획이 수집된 이후에 실행되어야 함.
    main.py에서 "Mode 2"를 호출하는 방식을 사용하고 있음.
    경주 계획이 모두 수집되었는지 확인한 후에 실행되어야 함.

    6월 6일 이슈사항 -> 프로세스가 2개가 띄워져서 재시도가 3번 되었음. 총 7회의 API call이 발생함. 
    테스트 출력 찍어봐야하는 부분.

    """

    success = False
    for attempt in range(1, max_retries + 1):
        if attempt > 1: logger.info(f"--- [Mode 2 재시도] {attempt}/{max_retries} 회차 ---")

        res = run_subprocess_with_logging([sys.executable, "main.py", url], cwd=ENTRY_SHEET_DIR)
        
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

def validate_result_csv_data(date, venue):
    """결과 CSV 파일을 열어 12경주의 순위(RK) 또는 착차(MARGIN) 데이터가 존재하는지 검증 (경기가 모두 끝났는지 확인)"""
    csv_path = WEB_CRAWLER_DIR / "data" / f"race_planning_{venue}_{date}.csv"
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
            
            # 마지막 경주번호 찾기 (보통 12)
            rcnos = [int(row.get('RCNO', 0)) for row in rows if row.get('RCNO', '').isdigit()]
            if not rcnos:
                return False
                
            last_rcno = max(rcnos)
            if last_rcno < 12:
                 logger.warning(f"  [검증 실패] {csv_path.name} - 아직 12경주 데이터가 생성되지 않았습니다.")
                 return False
            
            for row in rows:
                if str(row.get('RCNO', '')).zfill(2) == str(last_rcno).zfill(2) or row.get('RCNO') == str(last_rcno):
                    rk = row.get('RK', '').strip()
                    margin = row.get('MARGIN', '').strip()
                    # 취소마 등은 RK가 없을 수 있으므로 MARGIN(취소 등)이라도 있는지 확인
                    if not rk and not margin:
                        logger.warning(f"  [검증 실패] {csv_path.name} - 마지막 경주({last_rcno}R) 결과 데이터 누락 (RK/MARGIN 없음)")
                        return False
        return True
    except Exception as e:
        logger.error(f"  [검증 에러] {e}")
        return False


import threading

def _api_call_thread(url):
    try:
        # 백그라운드에서 최대 1시간까지 기다려보되, 에러가 나도 무시함
        requests.get(url, timeout=3600)
    except Exception as e:
        logger.warning(f"백그라운드 API 호출 중 연결 끊김 (서버 작업은 계속될 수 있음): {e}")

def trigger_external_api(date, venue, is_last=False):
    """외부 AI 예측 시스템 API 호출 (비동기 및 25분 강제 대기 방식)"""
    app_env = os.environ.get("APP_ENV", "prod").lower()
    if app_env == "test":
        logger.info(f"⚙️  [실행 환경] 테스트 환경이므로 외부 API 호출을 건너뜁니다. (대기 없이 즉시 완료 처리) [대상: {date} {venue}]")
        return True

    url = f"https://j.mafeel.ai/schedule/deploy/oneRaceDt.do?meet={venue}&raceDt={date}"
    logger.info(f"🚀 외부 API 백그라운드 호출 시작: {url}")
    
    # API 요청을 백그라운드로 발송
    t = threading.Thread(target=_api_call_thread, args=(url,))
    t.daemon = False # 파이썬 메인 스레드가 끝나도 API 응답을 기다리며 연결을 유지함
    t.start()
    
    if is_last:
        logger.info("⏳ 마지막 경기장의 API 호출을 전송했습니다. 대기 없이 종료 절차로 넘어갑니다.")
    else:
        # 메인 흐름은 무조건 30분 대기
        logger.info("⏳ API 호출 요청을 전송했습니다. 지금부터 25분(1500초) 대기를 시작합니다...")
        time.sleep(1500)
        logger.info("⏳ 25분 대기가 완료되었습니다. 다음 단계로 넘어갑니다.")
        
    return True

def disconnect_vpn_and_wait():
    """크롤링이 끝난 후 DB 및 API 통신을 위해 VPN을 해제하고 10초간 대기합니다."""
    nordvpn_path = r"C:\Program Files\NordVPN\nordvpn.exe"
    if Path(nordvpn_path).exists():
        logger.info("🌐 [보안] 크롤링 종료. DB/API 접근을 위해 VPN을 해제하고 10초 안정화 대기합니다...")
        subprocess.run([nordvpn_path, "-d"], shell=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        time.sleep(10)

def run_automation_pipeline(mode):
    """완전 자동화 파이프라인 (최대 1회 재시도, range(1, 3))"""
    day_name = "토요일" if mode == "2" else "일요일" # mode2:토요일, mode3:일요일 
    weekday_target = 5 if mode == "2" else 6 # 5:토요일, 6:일요일 
    
    logger.info(f"\n{'='*60}\n🤖 {day_name} 경기 자동화 파이프라인 가동 (최대 1회 재시도)\n{'='*60}")
    
    max_retries = 1
    retry_delay = 1 if os.environ.get("APP_ENV", "prod").lower() == "test" else 600  # 테스트 환경에선 1초, 평소엔 10분 대기
    
    success = False
    for attempt in range(1, 3):  # 1, 2 총 2회 시도
        logger.info(f"\n[시간: {datetime.now().strftime('%H:%M:%S')}] 데이터 스캔 및 수집 시도 중... ({attempt}/2 회차)")
        
        # 1. 대상 탐색
        all_targets = get_all_target_races()
        targets = [t for t in all_targets if datetime.strptime(t['date'], "%Y%m%d").weekday() == weekday_target]
        
        if not targets:
            if attempt < 2:
                logger.warning(f"🔍 탐색된 {day_name} 경기가 없습니다. {retry_delay}초 후 다시 시도합니다.")
                time.sleep(retry_delay)
                continue
            else:
                logger.error("❌ 재시도했으나 대상 경기가 발견되지 않아 자동화를 취소합니다.")
                sys.exit(0)
        
        # 2. 날씨 및 메인 크롤링 실행 (기존 로직 활용)
        process_plan_targets(targets)
        
        # 3. 데이터 완성도 검증
        all_valid = True
        for t in targets:
            if not validate_csv_data(t['date'], t['venue']):
                all_valid = False
                break
        
        if not all_valid:
            if attempt < 2:
                logger.info(f"⚠️ 일부 데이터가 아직 미완성 상태입니다. {retry_delay}초 후 다시 시도합니다.")
                time.sleep(retry_delay)
                continue
            else:
                logger.error("❌ 재시도했으나 일부 데이터가 여전히 미완성 상태입니다. 종료합니다.")
                sys.exit(1)
        
        # 모든 검증을 정상 통과한 경우 루프 탈출
        success = True
        break

    if success:
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
        
        # 크롤링 완료 후 VPN 연결 해제 (DB/API는 로컬 IP로 접근)
        disconnect_vpn_and_wait()
        
        # 4. DB 업로드 (6번) 및 API 이관 (7번) - 경기장별 순차 처리
        # 중복 실행 방지를 위해 유니크한 날짜/경기장 조합 추출
        processed_combos = []
        for t in targets:
            combo = (t['date'], t['venue'])
            if combo not in processed_combos:
                processed_combos.append(combo)
        
        # (날짜, 경기장) 의 combo 형식으로 담긴 리스트를 순회하면서 반복
        for idx, combo in enumerate(processed_combos):
            t_date, t_venue = combo
            is_last_item = (idx == len(processed_combos) - 1)
            
            # DB 업로드
            if run_mode_6(t_date, t_venue):
                send_telegram_message(f"✅ [{t_date} {t_venue}] CSV를 DB tmp 테이블에 적재 완료!")
            else:
                send_telegram_message(f"❌ [{t_date} {t_venue}] DB tmp 테이블 적재 도중 실패!")
                continue
                
            # API 이관
            if run_mode_7(t_date, t_venue):
                send_telegram_message(f"✅ [{t_date} {t_venue}] tmp 데이터를 API 테이블에 완벽히 이관 완료!")
            else:
                send_telegram_message(f"❌ [{t_date} {t_venue}] API 테이블 이관 실패!")
                continue
                
            # 외부 API 트리거
            if trigger_external_api(t_date, t_venue, is_last=is_last_item):
                send_telegram_message(f"🚀 [{t_date} {t_venue}] API 백그라운드 호출 전송 완료!")
            else:
                send_telegram_message(f"⚠️ [{t_date} {t_venue}] API 호출 전송 실패!")
        
        logger.info(f"\n✅ {day_name} 모든 자동화 작업이 성공적으로 종료되었습니다. 프로그램을 종료합니다.")
        sys.exit(0)

def run_result_automation_pipeline(mode):
    """과거 결과 자동화 모드 (최대 1회 재시도, range(1, 3))"""
    from datetime import datetime, timedelta
    
    target_weekday = 5 if mode == "4" else 6
    day_name = "토요일 결과" if mode == "4" else "일요일 결과"
    
    today = datetime.today()
    days_diff = target_weekday - today.weekday()
    if days_diff > 0:
        days_diff -= 7  # 오늘 기준 지난(혹은 오늘) 요일을 찾음
    target_date = today + timedelta(days=days_diff)
    target_date_str = target_date.strftime("%Y%m%d")

    send_telegram_message(f"🚀 [{day_name}] 결과 데이터 자동화 파이프라인 시작 (대상: {target_date_str})")
    logger.info(f"========== {day_name} 자동화 시작: 타겟 날짜 {target_date_str} ==========")
    
    max_retries = 1
    retry_delay = 1 if os.environ.get("APP_ENV", "prod").lower() == "test" else 600  # 테스트 환경에선 1초, 평소엔 10분 대기
    
    success = False
    for attempt in range(1, 3):  # 1, 2 총 2회 시도
        logger.info(f"🔍 {target_date_str} 결과 데이터 스캔 중... ({attempt}/2 회차)")
        targets = discover_races(target_date_str)
        
        if not targets:
            if attempt < 2:
                logger.info(f"아직 대상 데이터가 없습니다. {retry_delay}초 후 다시 스캔합니다.")
                time.sleep(retry_delay)
                continue
            else:
                logger.error("❌ 재시도했으나 대상 결과 데이터가 발견되지 않아 취소합니다.")
                sys.exit(0)
                
        all_completed = True
        
        for t in targets:
            logger.info(f"▶ 타겟 확인: {t['date']} {t['venue']} (URL: {t['url']})")
            
            # 1. 경기 결과 수집 (Phase 1)
            run_mode_1_logic(t['url'])
            
            # 2. 수집된 결과 CSV 검증 (모든 경주가 종료되었는지)
            is_valid = validate_result_csv_data(t['date'], t['venue'])
            if not is_valid:
                logger.warning(f"⚠️ {t['venue']} - 아직 경기 결과가 완벽하게 집계되지 않았습니다.")
                all_completed = False
                
        if not all_completed:
            if attempt < 2:
                logger.info(f"⏳ 일부 경기장 결과가 미완성입니다. {retry_delay}초 후 다시 스캔 및 검증을 시도합니다.")
                time.sleep(retry_delay)
                continue
            else:
                logger.error("❌ 재시도했으나 일부 경기장 결과가 여전히 미완성입니다. 종료합니다.")
                sys.exit(1)
                
        # 모든 검증 통과 시 루프 탈출
        success = True
        break
        
    if success:
        logger.info("✨ 모든 경기 결과 데이터 수집 및 검증 완료! 하위 상세 데이터 수집을 시작합니다.")
        
        # 3. 하위 디테일 크롤러 가동 (모든 경기장 대상)
        child_success = True
        for t in targets:
            suffix = f"{t['venue']}_{t['date']}"
            if run_child_crawlers(suffix) is False:
                child_success = False

        if child_success:
            send_telegram_message(f"✅ [{day_name}] 모든 결과 데이터를 확실히 CSV로 저장 완료!")
        else:
            send_telegram_message(f"❌ [{day_name}] 일부 결과 CSV 데이터 수집 실패! (DB 적재는 가능한 대상만 진행합니다)")

        logger.info("✨ 결과 데이터 수집 및 검증 완료! 후속 작업을 진행합니다.")
        
        # 크롤링 완료 후 VPN 연결 해제 (DB/API는 로컬 IP로 접근)
        disconnect_vpn_and_wait()
        
        # 4. DB 업로드 (8번) 및 API 이관 (9번) - 경기장별 순차 처리
        processed_combos = []
        for t in targets:
            combo = (t['date'], t['venue'])
            if combo not in processed_combos:
                # DB 업로드
                if run_mode_8(t['date'], t['venue']):
                    send_telegram_message(f"✅ [{t['date']} {t['venue']}] 결과 CSV를 DB tmp_races 테이블에 적재 완료!")
                else:
                    send_telegram_message(f"❌ [{t['date']} {t['venue']}] DB 결과 테이블 적재 도중 실패!")
                    continue
                    
                # API 이관
                if run_mode_9(t['date'], t['venue']):
                    send_telegram_message(f"✅ [{t['date']} {t['venue']}] 결과 데이터를 API 테이블에 완벽히 이관 완료!")
                else:
                    send_telegram_message(f"❌ [{t['date']} {t['venue']}] API 테이블 이관 실패!")
                    continue
                    
                processed_combos.append(combo)
        
        send_telegram_message(f"🚀 [{day_name}] 모든 결과 데이터 처리 파이프라인 무사 종료!")
        
        # 5. 결과 리포트 자동 생성 및 발송 (Phase 6)
        for (t_date, t_venue_jp) in processed_combos:
            if run_reporting_pipeline(t_venue_jp, t_date):
                send_telegram_message(f"📧 [{t_venue_jp}-{t_date}] 결과 리포트 이메일 발송 완료!")
            else:
                send_telegram_message(f"⚠️ [{t_venue_jp}-{t_date}] 예측 메일을 찾지 못해 리포트 발송을 건너뛰었습니다.")
                
        logger.info(f"\n✅ {day_name} 모든 결과 자동화 작업이 성공적으로 종료되었습니다. 프로그램을 종료합니다.")
        sys.exit(0)

def run_lap_time_automation():
    """지난 주말(토, 일)의 구간 기록을 자동으로 수집하여 DB에 반영합니다. (1번 모드 로직 사용)"""
    today = datetime.now()
    # 지난주 일요일: 오늘 기준 가장 가까운 과거 일요일
    days_to_sunday = (today.weekday() - 6) % 7
    if days_to_sunday == 0: days_to_sunday = 7
    
    last_sunday = today - timedelta(days=days_to_sunday)
    last_saturday = last_sunday - timedelta(days=1)
    
    target_dates = [last_saturday.strftime("%Y%m%d"), last_sunday.strftime("%Y%m%d")]
    
    logger.info(f"🚀 지난 주말 구간 기록 자동 업데이트 시작 (대상: {target_dates})")
    
    for date in target_dates:
        # 해당 날짜의 경기장 URL 탐색
        found_races = discover_races(date)
        if not found_races:
            logger.warning(f"⚠️ {date}에 탐색된 경기가 없습니다.")
            continue

        for r in found_races:
            venue_jp = r['venue']
            # VENUE_MAP에서 한글 경기장명을 찾아 로그에 표시 (선택사항)
            venue_kor = next((k for k, v in VENUE_MAP.items() if v == venue_jp), venue_jp)
            
            logger.info(f"\n--- 자동 업데이트 시도: {date} {venue_kor} ({r['url']}) ---")
            # 1번 모드 수집 로직 실행
            run_mode_1_logic(r['url'])
            
    send_telegram_message(f"✅ 지난 주말({target_dates[0]}~{target_dates[1]}) 구간 기록 자동 업데이트 완료!")
    logger.info("✅ 모든 자동 업데이트 작업이 완료되었습니다.")

def run_kor_result_reporting_pipeline():
    """한국(서울) 리포트 자동 생성 및 발송 파이프라인 (--auto 7)"""
    from datetime import datetime, timedelta
    
    today = datetime.today()
    # 오늘이 토요일(5) 또는 일요일(6)이면 오늘 날짜를 그대로 사용
    # 그 외 평일이면 가장 최근의 일요일(6)을 타겟으로 함
    if today.weekday() in [5, 6]:
        target_date = today
    else:
        days_diff = 6 - today.weekday()
        if days_diff > 0:
            days_diff -= 7
        target_date = today + timedelta(days=days_diff)
        
    target_date_str = target_date.strftime("%Y%m%d")
    
    send_telegram_message(f"🚀 [한국 결과] 리포트 자동화 파이프라인 시작 (대상: {target_date_str})")
    logger.info(f"========== 한국 결과 리포트 자동화 시작: 타겟 날짜 {target_date_str} ==========")
    
    try:
        from src.reporting.Reporting.email_report_kor import run_kor_reporting_pipeline
        for kor_venue in ["서울"]:
            if run_kor_reporting_pipeline(kor_venue, target_date_str):
                send_telegram_message(f"📧 [{kor_venue}] 한국 결과 리포트 이메일 발송 완료! (대상: {target_date_str})")
            else:
                logger.info(f"⚠️ [{kor_venue}] 최근 예측 메일이 없거나 처리할 수 없어 리포트 발송을 건너뜁니다. (대상: {target_date_str})")
    except Exception as e:
        logger.error(f"한국 결과 리포트 발송 중 에러 발생: {e}")
        send_telegram_message(f"❌ 한국 결과 리포트 발송 중 에러 발생: {e}")
        
    logger.info(f"\n✅ 한국 결과 자동화 작업이 성공적으로 종료되었습니다. 프로그램을 종료합니다.")
    sys.exit(0)

from datetime import timedelta

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
        run_subprocess_with_logging([sys.executable, "main.py"], cwd=INFO_DIR)

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
            
        res = run_subprocess_with_logging(cmd, cwd=DB_DIR)
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
            
        res = run_subprocess_with_logging(cmd, cwd=DB_DIR)
        if res.returncode == 0:
            logger.info("\n========== 데이터 이관 작업이 완료되었습니다. ==========")
            return True
        else:
            logger.warning(f"⚠️ API 테이블 이관 실패 (종료코드: {res.returncode})")
            
        if attempt < max_retries:
            logger.warning("⚠️ 5초 후 API 테이블 이관을 재시도합니다.")
            time.sleep(5)
            
    return False

def run_mode_8(date=None, venue=None, max_retries=3):
    """8번 모드: 과거 경기 결과 CSV 데이터를 DB에 업로드 (DELETE & INSERT)"""
    logger.info(f"\n========== [Phase 4-Result] 과거 경기 결과 CSV DB 업로드 시작 ==========")
    if not (DB_DIR / "mariadb_result_upsert.py").exists():
        logger.error(f"[오류] mariadb_result_upsert.py 파일을 찾을 수 없습니다.")
        return False
        
    cmd = [sys.executable, "mariadb_result_upsert.py"]
    if date and venue:
        cmd.extend(["--date", date, "--venue", venue])
    else:
        # date와 venue가 없으면 기존처럼 input 받음 (수동 실행용)
        print("\n" + "┌" + "─" * 45 + "┐")
        print("│       [ 과거 경기 결과 DB 업로드 대상 입력 ]       │")
        print("├" + "─" * 45 + "┤")
        print("│  1. 날짜 입력 (형식: YYYYMMDD 예: 20260419)  │")
        print("│  2. 경기장 이름 (도쿄, 나카야마, 한신, 교토) │")
        print("└" + "─" * 45 + "┘")
        
        date_input = input("\n📅 수집된 날짜를 입력하세요: ").strip()
        if not re.match(r"^\d{8}$", date_input):
            print("❌ 오류: 날짜 형식이 올바르지 않습니다.")
            return False
        venue_input = input("🏟️ 경기장 이름을 입력하세요 (도쿄/나카야마/한신/교토): ").strip()
        if venue_input not in VENUE_MAP:
            print(f"❌ 오류: '{venue_input}'은(는) 지원하지 않는 경기장입니다.")
            return False
            
        japanese_venue = VENUE_MAP[venue_input]
        cmd.extend(["--date", date_input, "--venue", japanese_venue])
        
    for attempt in range(1, max_retries + 1):
        if attempt > 1:
            logger.info(f"--- [DB 업로드 재시도] {attempt}/{max_retries} 회차 ---")
            
        res = run_subprocess_with_logging(cmd, cwd=DB_DIR)
        if res.returncode == 0:
            logger.info("\n========== DB 업로드 작업이 완료되었습니다. ==========")
            return True
        else:
            logger.warning(f"⚠️ DB 업로드 실패 (종료코드: {res.returncode})")
            
        if attempt < max_retries:
            logger.warning("⚠️ 5초 후 DB 업로드를 재시도합니다.")
            time.sleep(5)
            
    return False

def run_mode_9(date=None, venue=None, max_retries=3):
    """9번 모드: 과거 경기 결과 임시 테이블 데이터를 API 테이블로 이관"""
    logger.info(f"\n========== [Phase 5-Result] 과거 경기 결과 API 이관 시작 ==========")
    if not (DB_DIR / "mariadb_result_api_transfer.py").exists():
        logger.error(f"[오류] mariadb_result_api_transfer.py 파일을 찾을 수 없습니다.")
        return False
        
    cmd = [sys.executable, "mariadb_result_api_transfer.py"]
    if date and venue:
        cmd.extend(["--date", date, "--venue", venue])
    else:
        print("\n" + "┌" + "─" * 45 + "┐")
        print("│       [ 과거 경기 결과 API 이관 대상 입력 ]        │")
        print("├" + "─" * 45 + "┤")
        print("│  1. 날짜 입력 (형식: YYYYMMDD 예: 20260419)  │")
        print("│  2. 경기장 이름 (도쿄, 나카야마, 한신, 교토) │")
        print("└" + "─" * 45 + "┘")
        
        date_input = input("\n📅 이관할 날짜를 입력하세요: ").strip()
        if not re.match(r"^\d{8}$", date_input):
            print("❌ 오류: 날짜 형식이 올바르지 않습니다.")
            return False
        venue_input = input("🏟️ 경기장 이름을 입력하세요 (도쿄/나카야마/한신/교토): ").strip()
        if venue_input not in VENUE_MAP:
            print(f"❌ 오류: '{venue_input}'은(는) 지원하지 않는 경기장입니다.")
            return False
            
        japanese_venue = VENUE_MAP[venue_input]
        cmd.extend(["--date", date_input, "--venue", japanese_venue])
        
    for attempt in range(1, max_retries + 1):
        if attempt > 1:
            logger.info(f"--- [API 이관 재시도] {attempt}/{max_retries} 회차 ---")
            
        res = run_subprocess_with_logging(cmd, cwd=DB_DIR)
        if res.returncode == 0:
            logger.info("\n========== 데이터 이관 작업이 완료되었습니다. ==========")
            return True
        else:
            logger.warning(f"⚠️ API 테이블 이관 실패 (종료코드: {res.returncode})")
            
        if attempt < max_retries:
            logger.warning("⚠️ 5초 후 API 테이블 이관을 재시도합니다.")
            time.sleep(5)
            
    return False

def run_mode_10():
    """10번 모드: 특정 날짜와 경기장의 구간 기록(Lap Time) 업데이트 (기존 1번 모드 로직 활용)"""
    print("\n" + "┌" + "─" * 45 + "┐")
    print("│      [ 지난 주 구간 기록 업데이트 입력 ]       │")
    print("├" + "─" * 45 + "┤")
    print("│  1. 날짜 입력 (형식: YYYYMMDD 예: 20260419)  │")
    print("│  2. 경기장 이름 (도쿄, 나카야마, 한신, 교토) │")
    print("└" + "─" * 45 + "┘")
    
    date_input = input("\n📅 업데이트할 날짜를 입력하세요: ").strip()
    if not re.match(r"^\d{8}$", date_input):
        print("❌ 오류: 날짜 형식이 올바르지 않습니다.")
        return
    venue_input = input("🏟️ 경기장 이름을 입력하세요 (도쿄/나카야마/한신/교토): ").strip()
    if venue_input not in VENUE_MAP:
        print(f"❌ 오류: '{venue_input}'은(는) 지원하지 않는 경기장입니다.")
        return

    japanese_venue = VENUE_MAP[venue_input]
    logger.info(f"🔍 [Lap Time 업데이트] 자동 탐색 시작: {date_input} {venue_input}({japanese_venue})")

    # discovery.py 기능을 활용해 1R URL 확보
    found_races = discover_races(date_input)
    target_race = next((r for r in found_races if r['venue'] == japanese_venue), None)

    if not target_race:
        print(f"⚠️ 결과: 해당 날짜({date_input})에 {venue_input} 경기가 열리지 않았거나 찾을 수 없습니다.")
        return

    logger.info(f"🎯 탐색 성공! 1경기 URL: {target_race['url']}")
    
    # 결과 수집 프로세스 가동 (1번 모드 로직인 run_mode_1_logic 사용)
    run_mode_1_logic(target_race['url'])
    logger.info("\n========== 업데이트 작업이 완료되었습니다. ==========")

def run_mode_11():
    """11번 모드: [협업자용] 경주마 원본 사진 다운로더"""
    print("\n" + "┌" + "─" * 45 + "┐")
    print("│      [ 경주마 원본 사진 다운로더 대상 입력 ]     │")
    print("├" + "─" * 45 + "┤")
    print("│  1. 날짜 입력 (형식: YYYYMMDD 예: 20260419)  │")
    print("│  2. 경기장 이름 (도쿄, 나카야마, 한신, 교토) │")
    print("└" + "─" * 45 + "┘")
    
    date_input = input("\n📅 대상 날짜를 입력하세요: ").strip()
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
        run_subprocess_with_logging([sys.executable, "image_downloader.py", "--csv", str(csv_path)], cwd=HR_DIR)
        logger.info("\n========== 다운로드 작업이 완료되었습니다. ==========")

def print_discovery_results(targets):
    print("\n" + "-" * 50)
    print(f"자동 탐색 성공! 총 {len(targets)}개의 대상을 찾았습니다.")
    print("-" * 50)
    for i, t in enumerate(targets, 1):
        print(f" {i}. [{t['date']}] {t['venue']} | {t['url']}")
    print("-" * 50 + "\n")

def main():
    # 실행 환경(개발/테스트) 확인 메시지 출력
    app_env = os.environ.get("APP_ENV", "prod").lower()
    if app_env == "test":
        logger.info("[RUN_ENV] Running in TEST database mode.")
    else:
        logger.info("[RUN_ENV] Running in PRODUCTION database mode.")

    import argparse
    parser = argparse.ArgumentParser(description="넷케이바 자동화 마스터 파이프라인")
    parser.add_argument("--auto", choices=["2", "3", "4", "5", "6", "7"], help="자동화 모드 실행 (2:토 계획, 3:일 계획, 4:토 결과, 5:일 결과, 6:지난주 구간기록, 7:한국 결과리포트)")
    args = parser.parse_args()

    from netkeiba_auth import cleanup_session

    # main.py 초기 실행시 프리미엄 세션 로딩 및 종료전까지 재사용??? 이 구족가 가능한가?
    
    try:
        # 0. 넷케이바 프리미엄 세션 자동 확인 및 강제 생성
        print("\n" + "="*60)
        print("넷케이바 프리미엄 세션 자동 확인 및 최신 쿠키 생성 중...")
        try:
            get_netkeiba_cookies(force_login=True)  #force_login = True로 무조건 로그인 강제 시도처리
            print("세션 준비 완료!")
        except Exception as e:
            print(f"자동 로그인 실패: {e}")
            print("일반 모드로 진행하거나 설정을 확인해 주세요.")
        print("="*60 + "\n")

        # 1. 자동화 모드 체크
        if args.auto in ["2", "3"]:
            run_automation_pipeline(args.auto)
            return
        elif args.auto in ["4", "5"]:
            run_result_automation_pipeline(args.auto)
            return
        elif args.auto == "6":
            run_lap_time_automation()
            return
        elif args.auto == "7":
            run_kor_result_reporting_pipeline()
            return

        # 2. 대화형 수동 모드 루프
        while True:
            print_menu()
            mode = input("선택하실 번호를 입력하세요 (1~11 또는 q): ").strip().lower()
            
            if mode == 'q':
                print("\n프로그램을 종료합니다.")
                break
                
            if mode not in [str(i) for i in range(1, 12)]:
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
                    d_in = input("날짜 (엔터: 전체): ").strip()
                    v_in = input("경기장 (엔터: 전체): ").strip()
                    japanese_v = VENUE_MAP.get(v_in) if v_in else None
                    run_mode_6(d_in if d_in else None, japanese_v)
                elif mode == "7":
                    print("\n" + "┌" + "─" * 45 + "┐")
                    print("│        [ DB API 테이블 이관 대상 입력 ]       │")
                    print("├" + "─" * 45 + "┤")
                    print("│  * 미입력 시 자동 탐색된 전체 대상 처리      │")
                    print("│  1. 날짜 (YYYYMMDD) / 2. 경기장 한글명       │")
                    print("└" + "─" * 45 + "┘")
                    d_in = input("날짜 (엔터: 전체): ").strip()
                    v_in = input("경기장 (엔터: 전체): ").strip()
                    japanese_v = VENUE_MAP.get(v_in) if v_in else None
                    run_mode_7(d_in if d_in else None, japanese_v)
                elif mode == "8":
                    run_mode_8()
                elif mode == "9":
                    run_mode_9()
                elif mode == "10":
                    run_mode_10()
                elif mode == "11":
                    run_mode_11()
                
                logger.info("\n[SUCCESS] Pipeline task completed successfully.")
            except KeyboardInterrupt:
                logger.warning("\n[ABORT] Execution was aborted by user.")
            except Exception as e:
                # 개별 모드 에러 시 상세 에러 처리
                err_msg = str(e)
                status_code_info = ""
                
                # 403 / 503 에러 등의 상태 코드 패턴 파악
                if "403" in err_msg or "Forbidden" in err_msg:
                    status_code_info = " (HTTP 403 Forbidden - 접속이 차단되었습니다.)"
                elif "503" in err_msg:
                    status_code_info = " (HTTP 503 Service Unavailable - 서버 임시 점검 혹은 과부하 상태입니다.)"
                
                logger.error(f"\n[FAILURE] Exception occurred during execution: {err_msg}{status_code_info}")
                
                # 수동 모드 내 에러도 스케줄러 대비 텔레그램 즉각 발송
                telegram_alert = (
                    f"[넷케이바 수동 파이프라인 에러]\n"
                    f"• 모드: {mode}번 모드 실행 중\n"
                    f"• 에러 내용: {err_msg}{status_code_info}\n"
                    f"• 발생 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                )
                send_telegram_message(telegram_alert)
            
            print("\n메뉴로 돌아가려면 엔터를 누르세요...")
            input()

    except KeyboardInterrupt:
        logger.warning("[ABORT] Master Pipeline execution was aborted by user.")
    except Exception as master_err:
        err_msg = str(master_err)
        status_code_info = ""
        
        # 403 / 503 등의 에러 코드 정보 추가 추출
        if "403" in err_msg or "Forbidden" in err_msg:
            status_code_info = " (HTTP 403 Forbidden - 접속이 차단되었습니다.)"
        elif "503" in err_msg:
            status_code_info = " (HTTP 503 Service Unavailable - 서버 임시 점검 혹은 과부하 상태입니다.)"
            
        logger.error(f"[CRITICAL] Master pipeline failed: {err_msg}{status_code_info}")
        
        # 텔레그램 발송
        auto_mode_name = args.auto if args.auto else "대화형 실행"
        telegram_alert = (
            f"[넷케이바 마스터 파이프라인 장애 경보]\n"
            f"• 실행 모드: {auto_mode_name}\n"
            f"• 에러 원인: {err_msg}{status_code_info}\n"
            f"• 발생 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )
        send_telegram_message(telegram_alert)
        
    finally:
        # 프로그램이 수동 종료(q)되었든, 자동화 파이프라인이 완료되었든, 에러가 발생했든 무조건 디스크 세션 파일을 비웁니다.
        cleanup_session()

if __name__ == "__main__":
    main()
