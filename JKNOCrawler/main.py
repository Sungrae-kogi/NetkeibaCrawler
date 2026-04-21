# main.py
from __future__ import annotations

import csv
import time
import random
import logging
from datetime import datetime
from pathlib import Path

import requests

from parser import (
    parse_jockey_profile,
    parse_jockey_result_stats,   # ✅ result.html 파싱 추가
    TABLE1_COLS,
    TABLE2_COLS,
    RESULT_STATS_COLS,
)

OUT_COLS = ["MEET", "JKNO", "JKNAME", "BIRTHDAY", "AGE"] + TABLE1_COLS + TABLE2_COLS + RESULT_STATS_COLS

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; netkeiba-jockey-crawler/1.0)"
}

# 로깅 설정
BASE_DIR = Path(__file__).resolve().parent
LOG_DIR = BASE_DIR.parent / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
date_str = datetime.now().strftime("%Y%m%d")
LOG_FILE = LOG_DIR / f"{date_str}_JKNO.log"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("JKNO")


from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

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

def fetch_url(url: str, timeout: int = 20) -> str:
    r = http.get(url, headers=HEADERS, timeout=timeout)
    r.raise_for_status()
    r.encoding = "EUC-JP"
    return r.text


def fetch_jockey_page(jkno: str, timeout: int = 20) -> str:
    return fetch_url(f"https://db.netkeiba.com/jockey/{jkno}/", timeout=timeout)


def fetch_jockey_result_page(jkno: str, timeout: int = 20) -> str:
    return fetch_url(f"https://db.netkeiba.com/jockey/result.html?id={jkno}", timeout=timeout)


def load_unique_jkno_csv(path: str = "data/JKNO.csv") -> list[str]:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"JKNO CSV not found: {p.resolve()}")

    jknos: list[str] = []
    with p.open("r", encoding="utf-8-sig", newline="") as f:
        sample = f.read(2048)
        f.seek(0)

        has_comma = "," in sample
        if has_comma:
            reader = csv.DictReader(f)
            fieldnames = [fn.strip() for fn in (reader.fieldnames or [])]
            jk_field = None
            for cand in ["JKNO", "jkno", "JOCKEY_NO", "jockey_no"]:
                if cand in fieldnames:
                    jk_field = cand
                    break

            if jk_field:
                for row in reader:
                    v = (row.get(jk_field) or "").strip()
                    if v:
                        jknos.append(v)
            else:
                f.seek(0)
                r2 = csv.reader(f)
                for row in r2:
                    if not row:
                        continue
                    v = (row[0] or "").strip()
                    if v.lower() in ("jkno", "jockey_no", "jockeyno"):
                        continue
                    if v:
                        jknos.append(v)
        else:
            for line in f:
                v = line.strip()
                if not v or v.lower() in ("jkno", "jockey_no", "jockeyno"):
                    continue
                jknos.append(v)

    seen = set()
    uniq: list[str] = []
    for x in jknos:
        if x not in seen:
            seen.add(x)
            uniq.append(x)

    logger.info(f"[INFO] loaded JKNO count={len(uniq)} from {p.as_posix()}")
    return uniq


def get_completed_jknos(out_path: Path) -> set[str]:
    if not out_path.exists():
        return set()

    completed = set()
    with open(out_path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            val = row.get("JKNO")
            if val:
                completed.add(val.strip())
    return completed

def append_row_to_csv(path: str | Path, row: dict[str, str]) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    file_exists = p.exists()

    with p.open("a", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=OUT_COLS)
        if not file_exists:
            w.writeheader()
        fixed = {k: ("" if row.get(k) is None else row.get(k, "")) for k in OUT_COLS}
        w.writerow(fixed)


import sys

def main():
    base_dir = Path(__file__).resolve().parent
    date_suffix = sys.argv[1] if len(sys.argv) > 1 else "unknown"
    meet_name = date_suffix.split('_')[0] if '_' in date_suffix else "unknown"
    
    in_csv = base_dir / "nodata" / f"JKNO_{date_suffix}_list.csv"
    out_csv = base_dir / "data" / f"JKNO_result_{date_suffix}.csv"

    jknos = load_unique_jkno_csv(str(in_csv))
    completed_set = get_completed_jknos(out_csv)
    target_jknos = [jk for jk in jknos if jk not in completed_set]

    logger.info(f"전체 명단: {len(jknos)} 건")
    logger.info(f"이미 완료: {len(completed_set)} 건")
    logger.info(f"진행 대상: {len(target_jknos)} 건")

    if not target_jknos:
        logger.info("🎉 모든 크롤링이 이미 완료되었습니다!")
        return

    total = len(target_jknos)
    failed_jknos = []
    
    for i, jkno in enumerate(target_jknos, start=1):
        try:
            html_profile = fetch_jockey_page(jkno)
            html_result = fetch_jockey_result_page(jkno)

            row = parse_jockey_profile(html_profile, jkno=jkno, debug=False)

            # ✅ result.html 매핑 값 추가
            stat = parse_jockey_result_stats(html_result, jkno=jkno, debug=False)
            row.update(stat)
            row["MEET"] = meet_name

            append_row_to_csv(out_csv, row)
            logger.info(f"[{i}/{total}] OK JKNO={jkno}")

        except Exception as e:
            logger.error(f"[{i}/{total}] FAIL JKNO={jkno} err={e}")
            failed_jknos.append(jkno)

        time.sleep(random.uniform(0.7, 1.5))

    if failed_jknos:
        logger.warning(f"수집 완료되었으나, {len(failed_jknos)}건의 실패가 있었습니다. (결과 파일: {out_csv})")
        sys.exit(2)
    else:
        logger.info(f"🎉 크롤링 종료! 누락 없이 완벽하게 수집되었습니다. 결과 파일: {out_csv}")


if __name__ == "__main__":
    main()
