import os
import re
import csv
import logging
import sys
from datetime import datetime
from pathlib import Path

from parser import parse_race_page_rows

# 로깅 설정
BASE_DIR = Path(__file__).resolve().parent
LOG_DIR = BASE_DIR.parent / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
date_str = datetime.now().strftime("%Y%m%d")
LOG_FILE = LOG_DIR / f"{date_str}_WebCrawler.log"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler()
    ],
    force=True
)
logger = logging.getLogger("WebCrawler")

def make_race_urls(start_url: str, max_races: int = 18):
    m = re.search(r"race_id=(\d+)", start_url)
    if not m:
        raise ValueError("start_url에서 race_id를 찾지 못했습니다.")

    race_id = m.group(1)
    base = race_id[:-2]

    urls = []
    for r in range(1, max_races + 1):
        rid = f"{base}{r:02d}"
        urls.append(
            (r, f"https://race.netkeiba.com/race/result.html?race_id={rid}")
        )
    return urls

def save_rows_to_csv(rows: list[dict], filename: str):
    if not rows:
        return

    data_dir = Path(__file__).resolve().parent / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    filepath = data_dir / filename

    fieldnames = list(rows[0].keys())
    # 항상 덮어쓰기 (사용자 요청)
    with open(filepath, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    logger.info(f"CSV 저장 완료(Overwrite): {filepath} (총 {len(rows)} rows)")


from playwright.sync_api import sync_playwright


if __name__ == "__main__":
    if len(sys.argv) > 1:
        start_url = sys.argv[1]
    else:
        start_url = "https://race.netkeiba.com/race/result.html?race_id=202606030701&rf=race_list"
    
    max_races = 12
    
    state_path = BASE_DIR.parent / "storage_state.json"
    
    all_rows = []
    any_failed = False
    
    logger.info("🔑 Playwright 프리미엄 세션을 로드하여 결과 수집을 시작합니다...")
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context_args = {}
        if state_path.exists():
            context_args["storage_state"] = str(state_path)
            
        context = browser.new_context(**context_args)
        page = context.new_page()
        
        for rcno, url in make_race_urls(start_url, max_races=max_races):
            try:
                page.goto(url, timeout=30000)
                # 안정적인 로드를 위해 대기
                page.wait_for_load_state("domcontentloaded")
                
                # HTML 추출 후 파서에 전달
                html = page.content()
                rows = parse_race_page_rows(url, html=html)
                all_rows.extend(rows)
                logger.info(f"수집: {url} -> {len(rows)} rows")
            except Exception as e:
                logger.error(f"실패: {url} / {e}")
                any_failed = True
                
        browser.close()

    if all_rows:
        first_row = all_rows[0]
        date_raw = str(first_row.get("RCDATE") or "unknown").replace("-", "").strip()
        meet_str = str(first_row.get("MEET") or "unknown").strip()
        filename = f"race_planning_{meet_str}_{date_raw}.csv"
        save_rows_to_csv(all_rows, filename)
    else:
        logger.warning("수집된 데이터가 없습니다.")

    if any_failed:
        sys.exit(2)