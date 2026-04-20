import os
import re
import csv
import logging
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

"""
    도쿄 경기의 출전표 및 경기결과 데이터 수집 (마스터 파일 이어쓰기)
"""

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
            f"https://race.netkeiba.com/race/result.html?race_id={rid}"
        )
    return urls

def save_rows_to_csv(rows: list[dict], filename: str):
    if not rows:
        logger.warning("저장할 데이터가 없습니다.")
        return

    # WebCrawler/data 폴더 준비
    data_dir = Path(__file__).resolve().parent / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    filepath = data_dir / filename

    fieldnames = list(rows[0].keys())
    file_exists = filepath.exists()

    with open(filepath, "a", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerows(rows)

    logger.info(f"CSV 저장 완료: {filepath} (+{len(rows)} rows)")


import sys
if __name__ == "__main__":
    if len(sys.argv) > 1:
        start_url = sys.argv[1]
    else:
        start_url = "https://race.netkeiba.com/race/result.html?race_id=202606030701&rf=race_list"
    
    max_races = 12

    # (주의) 실제 운영 시 프리미엄 쿠키는 외부에 노출되지 않도록 환경변수 등으로 관리하는 것을 권장합니다.
    my_premium_cookie = "_pubcid=c611a6aa-a25a-4bee-9335-fce92f1cb85b; _pubcid_cst=zix7LPQsHA%3D%3D; cpt_ab_test3=control; _ga=GA1.3.43087749.1773647889; _im_vid=01KKTTE8FD2BJR6P3H96E0FTBR; _im_vid=01KKTTE8FD2BJR6P3H96E0FTBR; nkrace=31e8f6b8de6eb5e6104fa1e7d500ece3; _yjsu_yjad=1773648701.b1fbd492-9ba0-4398-a06f-55a6dea581d8; cpt_ab_test4=control; _im_uid.3929=i.7USgiS3PQiyKGfZggszH_A; __utmz=48494009.1774333075.6.2.utmccn=(referral)|utmcsr=nar.netkeiba.com|utmcct=/|utmcmd=referral; __binsUID=4fdc557e-4947-4bc9-802c-7bac720bd869; __utma=48494009.1363941113.1773732823.1774848759.1774853079.9; ga_netkeiba_member=Free; nkowner=31e8f6b8de6eb5e6104fa1e7d500ece3; _ga_TES9RDDPWZ=GS2.1.s1774853093$o2$g1$t1774853331$j44$l0$h0; _ga_W09XKKVWC0=GS2.1.s1774853093$o2$g1$t1774853331$j50$l0$h0; _ga_X3WZ5EPSWL=GS2.1.s1774853093$o2$g1$t1774853331$j50$l1$h1236888564; _ga_XNS3WYDQBF=GS2.1.s1774853093$o2$g1$t1774853331$j44$l0$h0; mbox=PC#f654e9904819443fbf8393e21b328403.32_0#1838098145|session#00bc61b2b3174550a8b800e06f906305#1774855205; netkeiba=TnprMU56TXpOUT09; nkauth=493fb15c03ab099205db392a3a9fe1e372523e170539fd7f5c348ae9; nd_ua=Windows%2010.0.0%3B%20%20Chromium%2F146%20Google%20Chrome%2F146; _ga=GA1.1.43087749.1773647889; cto_bidid=o33Lpl85SkdlaHBUZnJSJTJGUSUyQjI4OEVRZHRLdHRlNTBFSVJrRERDSE9BNERRJTJGbks1M1RZSSUyQkFOZXgzZWNWakVNZnBlbnlBRjJVeXJjM1Y1cnlvZnlSdHF2RiUyQjczNkxoYXd4ekl1MDJ3dHdqZHNsQlElM0Q; _gid=GA1.3.1019276714.1775440308; _dc_gtm_UA-2880481-1=1; __gads=ID=e823eb212d9fc053:T=1773647887:RT=1775440299:S=ALNI_MYhRL9Kx6x9ymkmt64zEYHyH2rHwQ; __gpi=UID=0000121fde8bd8ef:T=1773647887:RT=1775440299:S=ALNI_MaeFQWZ9egngdKluHSKD1AhFUfK_w; __eoi=ID=e15519fbf00b6ecf:T=1773647887:RT=1775440299:S=AA-AfjZyHNwaMTQoq9HOjkCfRtVZ; _clck=6qs9bv%5E2%5Eg4z%5E0%5E2266; cto_bundle=v-YsZF8wU0M3Y0VVbm5OUHEwZTVLRzVXaDhsenp1Z25oc2lKUGVlWk82bEQ3TTI2YnpDZUtud0I5NEpNd29OZ0hXZm8zdUVaNUY0ZmVrcTFMVEJ3VHhIVGNTY2xPZm5TNXZCajZ2RyUyQlFqZk1PZWdpM3pCVGlVMTR3UkhzdEJmdm1BdHB4OVFjJTJGWCUyRmolMkJjMmtVY3c0SEdGZDUlMkZnJTNEJTNE; url=https%3A%2F%2Frace.netkeiba.com%2Frace%2Fresult.html%3Frace_id%3D202606020501%26rf%3Drace_list; FCCDCF=%5Bnull%2Cnull%2Cnull%2Cnull%2Cnull%2Cnull%2C%5B%5B32%2C%22%5B%5C%22a3fcd35b-1e7d-4f3d-a97a-c62b6dc0164e%5C%22%2C%5B1773647889%2C233000000%5D%5D%22%5D%5D%5D; _ga_B2L5N4JT6V=GS2.1.s1775440308$o39$g1$t1775440314$j54$l0$h0; _ga_BQDXGQBP6X=GS2.1.s1775440308$o39$g1$t1775440314$j58$l1$h465467950; _clsk=bpwip2%5E1775440315243%5E5%5E0%5Eb.clarity.ms%2Fcollect; FCNEC=%5B%5B%22AKsRol-Ep8M5qsVtGL6g9btoM_wqfIE6br3BkROvyT3iZJPpdUDV6UAMBoGaWiIyeJHgyfKjnGYMkXeHDUkiDZlQN0ZrfbXrnfFKoaOTXZf2dFw8Yvfu7ZnwERLMUCmzFfoYDRjeTlExI9lXbMwzhs4ST3NdfVaCsw%3D%3D%22%5D%5D"
    all_rows = []

    for url in make_race_urls(start_url, max_races=max_races):
        try:
            rows = parse_race_page_rows(
                url, raw_cookie=my_premium_cookie
            )
            all_rows.extend(rows)
            logger.info(f"수집: {url} -> {len(rows)} rows")
        except Exception as e:
            logger.error(f"실패: {url} / {e}")

    if all_rows:
        first_row = all_rows[0]
        date_str = str(first_row.get("RCDATE") or first_row.get("RACE_DT") or first_row.get("date") or "unknown")
        date_str = date_str.replace("-", "").replace("/", "").strip()
        meet_str = str(first_row.get("MEET") or "unknown").strip()
        
        filename = f"race_planning_{meet_str}_{date_str}.csv"
        save_rows_to_csv(all_rows, filename)
    else:
        logger.warning("수집된 데이터가 없습니다.")