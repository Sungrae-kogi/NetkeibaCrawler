# main.py
from __future__ import annotations

import re
import sys
import time
import logging
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup


HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0 Safari/537.36"
    ),
    "Referer": "https://db.netkeiba.com/",
    "Accept-Language": "ja,en-US;q=0.9,en;q=0.8,ko;q=0.7",
}

BASE_TRAINER_RESULT_URL = "https://db.netkeiba.com/trainer/result.html?id={trno}"

# 로깅 설정
BASE_DIR = Path(__file__).resolve().parent
LOG_DIR = BASE_DIR.parent / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
date_str = datetime.now().strftime("%Y%m%d")
LOG_FILE = LOG_DIR / f"{date_str}_TRNO.log"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("TRNO")


# ----------------------------
# IO
# ----------------------------
def load_trno_list(csv_path: Path) -> list[str]:
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    try:
        df = pd.read_csv(csv_path, dtype={"TRNO": "string"}, encoding="utf-8-sig")
    except UnicodeDecodeError:
        df = pd.read_csv(csv_path, dtype={"TRNO": "string"}, encoding="cp932")
        
    if "TRNO" not in df.columns:
        raise ValueError("TRNO column not found")

    s = df["TRNO"].astype("string").str.strip()
    s = s[s.notna() & (s != "")]
    return s.tolist()


def save_csv(rows: list[dict], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(rows)
    df.to_csv(out_path, index=False, encoding="utf-8-sig")


# ----------------------------
# Network
# ----------------------------
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
http_session = requests.Session()
http_session.mount("https://", adapter)
http_session.mount("http://", adapter)

def fetch_html(url: str, session: requests.Session) -> str:
    resp = http_session.get(url, headers=HEADERS, timeout=15)
    resp.raise_for_status()

    # 넷케이바는 EUC-JP 인코딩을 사용하므로 수동 지정
    resp.encoding = "EUC-JP"

    return resp.text


# ----------------------------
# Parsing helpers
# ----------------------------
def norm_text(s: str | None) -> str | None:
    if s is None:
        return None
    return re.sub(r"\s+", " ", s).strip()


def clean_prname(name: str | None) -> str | None:
    if not name:
        return None

    s = name.strip()
    s = re.split(r"[（(]", s, maxsplit=1)[0].strip()
    s = re.sub(r"\s+", " ", s).strip()
    return s or None


def to_int(s: str | None) -> int | None:
    if s is None:
        return None
    s = s.strip()
    if not s:
        return None
    s = re.sub(r"[^\d,]", "", s)
    if not s:
        return None
    s = s.replace(",", "")
    try:
        return int(s)
    except ValueError:
        return None


def parse_name_block(soup: BeautifulSoup) -> tuple[str | None, str | None]:
    name_block = soup.select_one(".Name")
    if not name_block:
        return None, None

    h1 = name_block.find("h1")
    p = name_block.find("p")

    h1_raw = norm_text(h1.get_text(strip=True)) if h1 else None
    h1_text = clean_prname(h1_raw)

    p_text = norm_text(p.get_text(" ", strip=True)) if p else None
    return h1_text, p_text


def split_p_to_birthday_prgubun(p_text: str | None) -> tuple[str | None, str | None]:
    if not p_text:
        return None, None

    parts = p_text.split()
    if len(parts) >= 2:
        left = parts[0].replace("/", "").strip()
        right = parts[1].strip()
        return left or None, right or None

    return p_text.replace("/", "").strip() or None, None


def parse_race_table_trs(soup: BeautifulSoup) -> tuple[list[str] | None, list[str] | None]:
    container = soup.select_one("#contents_liquid")
    if not container:
        return None, None

    table = container.select_one(".race_table_01")
    if not table:
        return None, None

    tbody = table.find("tbody")
    if tbody:
        trs = tbody.find_all("tr")
    else:
        trs = table.find_all("tr")

    if len(trs) < 4:
        return None, None

    def tds_text_list(tr) -> list[str]:
        tds = tr.find_all("td")
        return [norm_text(td.get_text(" ", strip=True)) or "" for td in tds]

    tr3 = tds_text_list(trs[2])  # 3번째 tr
    tr4 = tds_text_list(trs[3])  # 4번째 tr
    return tr3, tr4


def safe_get(lst: list[str], idx: int) -> str | None:
    v = lst[idx] if 0 <= idx < len(lst) else None
    if v is None:
        return None
    v = v.strip()
    return v if v else None


def sum_cells_as_int(lst: list[str], idxs_0based: list[int]) -> int | None:
    total = 0
    found = False

    for i in idxs_0based:
        v = safe_get(lst, i)
        n = to_int(v) if v is not None else None
        if n is not None:
            total += n
            found = True

    return total if found else None


# ----------------------------
# Main mapping
# ----------------------------
def fetch_and_map(trno: str, session: requests.Session) -> dict:
    url = BASE_TRAINER_RESULT_URL.format(trno=trno)
    html = fetch_html(url, session)
    soup = BeautifulSoup(html, "html.parser")

    prname, p_text = parse_name_block(soup)
    birthday, prgubun = split_p_to_birthday_prgubun(p_text)

    tr3, tr4 = parse_race_table_trs(soup)

    idx_ord1, idx_ord2, idx_ord3 = 2, 3, 4          
    idx_rccnt = [6, 8, 10, 12, 14]                  
    idx_chaksun = 19                                

    row = {
        "PRNO": trno,
        "PRNAME": prname,
        "BIRTHDAY": birthday,
        "PRGUBUN": prgubun,

        "ORD1CNTT": None,
        "ORD2CNTT": None,
        "ORD3CNTT": None,
        "RCCNTT": None,
        "CHAKSUNT": None,

        "ORD1CNTY": None,
        "ORD2CNTY": None,
        "ORD3CNTY": None,
        "RCCNTY": None,
        "CHAKSUNY": None,
    }

    if tr3:
        row["ORD1CNTT"] = to_int(safe_get(tr3, idx_ord1))
        row["ORD2CNTT"] = to_int(safe_get(tr3, idx_ord2))
        row["ORD3CNTT"] = to_int(safe_get(tr3, idx_ord3))
        row["RCCNTT"] = sum_cells_as_int(tr3, idx_rccnt)
        row["CHAKSUNT"] = to_int(safe_get(tr3, idx_chaksun))

    if tr4:
        row["ORD1CNTY"] = to_int(safe_get(tr4, idx_ord1))
        row["ORD2CNTY"] = to_int(safe_get(tr4, idx_ord2))
        row["ORD3CNTY"] = to_int(safe_get(tr4, idx_ord3))
        row["RCCNTY"] = sum_cells_as_int(tr4, idx_rccnt)
        row["CHAKSUNY"] = to_int(safe_get(tr4, idx_chaksun))

    return row


def main() -> None:
    project_root = Path(__file__).resolve().parent
    date_suffix = sys.argv[1] if len(sys.argv) > 1 else "unknown"
    meet_name = date_suffix.split('_')[0] if '_' in date_suffix else "unknown"
    
    in_csv = project_root / "nodata" / f"TRNO_{date_suffix}_list.csv"
    out_csv = project_root / "data" / f"TRNO_result_{date_suffix}.csv"

    trno_list = load_trno_list(in_csv)
    test_list = trno_list

    rows: list[dict] = []
    total = len(test_list)
    with requests.Session() as session:
        for i, trno in enumerate(test_list, start=1):
            try:
                row = fetch_and_map(trno, session)
                new_row = {"MEET": meet_name}
                new_row.update(row)
                rows.append(new_row)
                logger.info(f"[{i}/{total}] OK TRNO={trno}")
            except Exception as e:
                rows.append({"MEET": meet_name, "PRNO": trno, "ERROR": f"{type(e).__name__}: {e}"})
                logger.error(f"[{i}/{total}] FAIL TRNO={trno} / {e}")
            time.sleep(0.5)

    save_csv(rows, out_csv)
    logger.info(f"Saved {len(rows)} rows -> {out_csv}")


if __name__ == "__main__":
    main()
