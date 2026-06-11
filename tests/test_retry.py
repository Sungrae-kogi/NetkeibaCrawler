import os
import sys
import time
import logging
from pathlib import Path

# 로깅 설정
BASE_DIR = Path(__file__).resolve().parent
LOG_DIR = BASE_DIR.parent / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / "test_master_retry.log"

# Tee 클래스 모방
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
original_stdout = sys.stdout
sys.stdout = Tee(sys.stdout, master_f)
sys.stderr = Tee(sys.stderr, master_f)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(original_stdout)
    ]
)
logger = logging.getLogger("TestMaster")

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

def simulate_mode_2_logic(max_retries: int = 3):
    logger.info("▶ [Phase 1 & 2] 경기 계획 수집 더미 테스트 시작")
    
    success = False
    for attempt in range(1, max_retries + 1):
        if attempt > 1:
            logger.info(f"--- [Mode 2 재시도] {attempt}/{max_retries} 회차 ---")
        
        # tests/dummy_child.py 를 실행하도록 설정
        dummy_path = BASE_DIR / "dummy_child.py"
        res = run_subprocess_with_logging([sys.executable, str(dummy_path)], cwd=BASE_DIR)
        
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

    if success:
        logger.info("🎉 경기 계획 수집 성공!")
    else:
        logger.error("❌ 경기 계획 수집 최종 실패!")

if __name__ == "__main__":
    simulate_mode_2_logic()
