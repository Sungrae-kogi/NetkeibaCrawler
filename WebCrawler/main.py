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
    ]
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


if __name__ == "__main__":
    if len(sys.argv) > 1:
        start_url = sys.argv[1]
    else:
        start_url = "https://race.netkeiba.com/race/result.html?race_id=202606030701&rf=race_list"
    
    max_races = 12
    my_premium_cookie = "_ga=GA1.3.43087749.1773647889; _im_vid=01KKTTE8FD2BJR6P3H96E0FTBR; _im_vid=01KKTTE8FD2BJR6P3H96E0FTBR; _yjsu_yjad=1773648701.b1fbd492-9ba0-4398-a06f-55a6dea581d8; _im_uid.3929=i.7USgiS3PQiyKGfZggszH_A; __binsUID=4fdc557e-4947-4bc9-802c-7bac720bd869; ga_netkeiba_member=Free; mbox=PC#f654e9904819443fbf8393e21b328403.32_0#1838098145|session#00bc61b2b3174550a8b800e06f906305#1774855205; cto_bidid=o33Lpl85SkdlaHBUZnJSJTJGUSUyQjI4OEVRZHRLdHRlNTBFSVJrRERDSE9BNERRJTJGbks1M1RZSSUyQkFOZXgzZWNWakVNZnBlbnlBRjJVeXJjM1Y1cnlvZnlSdHF2RiUyQjczNkxoYXd4ekl1MDJ3dHdqZHNsQlElM0Q; nd_ua=Windows%2010.0.0%3B%20%20Google%20Chrome%2F147%20Chromium%2F147; _gid=GA1.3.1485614291.1776219695; nkrace=2af769b432a50c024cf6e2a057601812; _ga_TES9RDDPWZ=GS2.1.s1776220614$o3$g0$t1776220614$j60$l0$h0; _ga_X3WZ5EPSWL=GS2.1.s1776220614$o3$g0$t1776220614$j60$l1$h2006083052; _ga_W09XKKVWC0=GS2.1.s1776220614$o3$g0$t1776220614$j60$l0$h0; _ga_XNS3WYDQBF=GS2.1.s1776220614$o3$g0$t1776220614$j60$l0$h0; _clck=1mprhi3%5E2%5Eg5f%5E0%5E2301; user_odds_20260422=ODUL%3A7b34b8714e71cf532f8a793862d6ce3408; umai_trial_out=1; __utma=48494009.1363941113.1773732823.1774853079.1776823361.10; __utmc=48494009; __utmz=48494009.1776823361.10.3.utmccn=(referral)|utmcsr=info.netkeiba.com|utmcct=/|utmcmd=referral; _gid=GA1.2.1641444119.1776823361; _ga=GA1.1.43087749.1773647889; cto_bundle=YgIwZ194dTdreER2ZEhFMWxSNzdNNkhGcnRYN0lZNUxIWXRuY3FsSDAxRlFRJTJCZ3VMSmNCbFhoMzhKV28zS0pzUkV3TFRNRGNQV1MlMkZaS1VRQkxRc0sxaVMyVGZkWTZvcW1mZVhIQWJGZFJ6Z050aHNRc3Y0blZiT0YwaE45VWdzR0VNZmFMS1NkNUN5WUJNUGZta0pXcnBuaHh3JTNEJTNE; FCCDCF=%5Bnull%2Cnull%2Cnull%2Cnull%2Cnull%2Cnull%2C%5B%5B32%2C%22%5B%5C%22171b98e3-9a81-4404-b843-acda29dda790%5C%22%2C%5B1776647267%2C294000000%5D%5D%22%5D%5D%5D; __gads=ID=5fae409398165a83:T=1776647265:RT=1776832036:S=ALNI_MZRyNcCxYZ7oaRjwAgoD06q68QIpw; __gpi=UID=0000126dca115b9e:T=1776647265:RT=1776832036:S=ALNI_MZtr55Ulg5SUp23Vq92U3rntK7USA; __eoi=ID=bccac6a98451cd0e:T=1776647265:RT=1776832036:S=AA-AfjaN75qD8eQR5bdFFPHwzlq6; FCNEC=%5B%5B%22AKsRol__pTfDDVffgLf3WV2fo4hWv1z_Nu806fLN0bhPf8CnxvCVc71e5dkxHBseok5SDTxb4WzcIb6CpFMFwGElcI54OyKOt0uOZ0VEP5jH2o_HYqnDCoJyLgc8c-PC9tbs63dNH9eV56rk6FdPhfUcmah5VqpnpQ%3D%3D%22%5D%5D; _clsk=1o97fmk%5E1776833958304%5E9%5E0%5Eb.clarity.ms%2Fcollect; _ga_B2L5N4JT6V=GS2.1.s1776833959$o13$g0$t1776833959$j60$l0$h0; _ga_BQDXGQBP6X=GS2.1.s1776833959$o13$g0$t1776833959$j60$l1$h1978159112; netkeiba=TnprMU56TXpOUT09; nkauth=f9c963fff3b3dbc54f78c0b339e65dc4d699651efa79f2995c348ae9; url=https%3A%2F%2Frace.netkeiba.com%2Frace%2Fshutuba.html%3Frace_id%3D202605020105"
    all_rows = []
    
    any_failed = False
    for rcno, url in make_race_urls(start_url, max_races=max_races):
        try:
            rows = parse_race_page_rows(url, raw_cookie=my_premium_cookie)
            all_rows.extend(rows)
            logger.info(f"수집: {url} -> {len(rows)} rows")
        except Exception as e:
            logger.error(f"실패: {url} / {e}")
            any_failed = True

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