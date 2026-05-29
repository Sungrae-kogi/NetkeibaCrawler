# main.py
from __future__ import annotations

import re
import sys
import time
import random
import logging
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup


HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; netkeiba-trainer-crawler/1.0)"
}

BASE_TRAINER_RESULT_URL = "https://db.netkeiba.com/trainer/result.html?id={trno}"

# 로깅 설정
BASE_DIR = Path(__file__).resolve().parent
LOG_DIR = BASE_DIR.parent.parent.parent / "logs"
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


import csv

def get_completed_trnos(out_path: Path) -> set[str]:
    if not out_path.exists():
        return set()

    completed = set()
    with open(out_path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            val = row.get("PRNO")
            if val:
                completed.add(val.strip())
    return completed


def append_row_to_csv(out_path: Path, row: dict) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    file_exists = out_path.exists()
    
    with open(out_path, "a", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=list(row.keys()))
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)


# ----------------------------
# Network & VPN Defense
# ----------------------------
import subprocess

def reconnect_vpn():
    nordvpn_path = r"C:\Program Files\NordVPN\nordvpn.exe"
    if not Path(nordvpn_path).exists():
        logger.error(f"NordVPN 실행 파일을 찾을 수 없습니다: {nordvpn_path}")
        return
        
    logger.info("🌐 [NordVPN] VPN 연결 해제 중...")
    try:
        subprocess.run([nordvpn_path, "-d"], shell=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        time.sleep(3)
        logger.info("🌐 [NordVPN] VPN 재연결 중 (Japan 서버)...")
        subprocess.run([nordvpn_path, "-c", "-g", "Japan"], shell=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        logger.info("🌐 [NordVPN] 재연결 완료! 네트워크 안정화를 위해 10초 대기합니다.")
        time.sleep(10)
    except Exception as e:
        logger.error(f"🌐 [NordVPN] 제어 중 예외 발생: {e}")

def ensure_vpn_connected():
    nordvpn_path = r"C:\Program Files\NordVPN\nordvpn.exe"
    if not Path(nordvpn_path).exists():
        return
        
    logger.info("🌐 [NordVPN] 초기 연결 상태를 확인하고 Japan 서버로 연결을 보장합니다...")
    try:
        subprocess.run([nordvpn_path, "-c", "-g", "Japan"], shell=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        time.sleep(5)
    except Exception as e:
        logger.error(f"🌐 [NordVPN] 초기 연결 중 예외 발생: {e}")

def fetch_html(url: str, session: requests.Session) -> str:
    max_retries = 3
    for attempt in range(1, max_retries + 1):
        try:
            resp = session.get(url, headers=HEADERS, timeout=15)
            if resp.status_code in [403, 503, 502, 504, 429]:
                logger.warning(f"⚠️ 접속 차단 또는 서버 에러(상태코드 {resp.status_code}). VPN 재연결 시도 ({attempt}/{max_retries})")
                reconnect_vpn()
                continue
            
            resp.raise_for_status()
            resp.encoding = "EUC-JP"
            return resp.text
            
        except requests.exceptions.Timeout:
            logger.warning(f"⚠️ 타임아웃 발생. VPN 재연결 시도 ({attempt}/{max_retries})")
            reconnect_vpn()
        except requests.exceptions.RequestException as e:
            logger.warning(f"⚠️ 요청 오류 발생: {e}. VPN 재연결 시도 ({attempt}/{max_retries})")
            reconnect_vpn()
            
    raise Exception(f"최대 재시도 횟수 초과: {url}")


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
    ensure_vpn_connected()
    project_root = Path(__file__).resolve().parent
    date_suffix = sys.argv[1] if len(sys.argv) > 1 else "unknown"
    meet_name = date_suffix.split('_')[0] if '_' in date_suffix else "unknown"
    
    in_csv = project_root / "nodata" / f"TRNO_{date_suffix}_list.csv"
    out_csv = project_root / "data" / f"TRNO_result_{date_suffix}.csv"

    trno_list = load_trno_list(in_csv)
    completed_set = get_completed_trnos(out_csv)
    target_trnos = [trno for trno in trno_list if trno not in completed_set]

    logger.info(f"전체 명단: {len(trno_list)} 건")
    logger.info(f"이미 완료: {len(completed_set)} 건")
    logger.info(f"진행 대상: {len(target_trnos)} 건")

    if not target_trnos:
        logger.info("🎉 모든 크롤링이 이미 완료되었습니다!")
        return

    failed_trnos = []
    total = len(target_trnos)
    with requests.Session() as session:
        for i, trno in enumerate(target_trnos, start=1):
            try:
                row = fetch_and_map(trno, session)
                new_row = {"MEET": meet_name}
                new_row.update(row)
                
                append_row_to_csv(out_csv, new_row)
                logger.info(f"[{i}/{total}] OK TRNO={trno}")
            except Exception as e:
                logger.error(f"[{i}/{total}] FAIL TRNO={trno} / {e}")
                failed_trnos.append(trno)
            time.sleep(random.uniform(2.5, 4.0))

    if failed_trnos:
        logger.warning(f"수집 완료되었으나, {len(failed_trnos)}건의 실패가 있었습니다. (결과 파일: {out_csv})")
        sys.exit(2)
    else:
        logger.info(f"🎉 크롤링 종료! 누락 없이 완벽하게 수집되었습니다. 결과 파일: {out_csv}")


if __name__ == "__main__":
    main()
