# main.py
from __future__ import annotations

import csv
import time
import random
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


def fetch_url(url: str, timeout: int = 20) -> str:
    r = requests.get(url, headers=HEADERS, timeout=timeout)
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

    print(f"[INFO] loaded JKNO count={len(uniq)} from {p.as_posix()}")
    return uniq


def write_csv(path: str, rows: list[dict[str, str]]) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)

    with p.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=OUT_COLS)
        w.writeheader()
        for row in rows:
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
    rows: list[dict[str, str]] = []
    
    for i, jkno in enumerate(jknos, start=1):
        print(f"[{i}/{len(jknos)}] jkno={jkno}")

        try:
            html_profile = fetch_jockey_page(jkno)
            html_result = fetch_jockey_result_page(jkno)

            debug = (i <= 2)
            row = parse_jockey_profile(html_profile, jkno=jkno, debug=debug)

            # ✅ result.html 매핑 값 추가
            stat = parse_jockey_result_stats(html_result, jkno=jkno, debug=debug)
            row.update(stat)
            row["MEET"] = meet_name

            rows.append(row)

        except Exception as e:
            print(f"[ERROR] jkno={jkno} err={e}")

        time.sleep(random.uniform(0.7, 1.5))

    write_csv(str(out_csv), rows)


if __name__ == "__main__":
    main()
