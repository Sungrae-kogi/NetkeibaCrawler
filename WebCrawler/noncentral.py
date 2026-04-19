import os
import re
import csv
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse


# =============================
# 설정: 시작 URL만 바꾸면 됨
# 예) https://nar.netkeiba.com/race/result.html?race_id=202644013001
# =============================
START_URL = "https://nar.netkeiba.com/race/result.html?race_id=202544100901&rf=race_list"


# -----------------------------
# Fetch
# -----------------------------
def fetch_html(url: str) -> str:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/121.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "ja,en;q=0.9,ko;q=0.8",
        "Referer": "https://nar.netkeiba.com/",
    }
    r = requests.get(url, headers=headers, timeout=20)
    r.raise_for_status()
    if not r.encoding or r.encoding.lower() == "iso-8859-1":
        r.encoding = r.apparent_encoding
    return r.text


# -----------------------------
# URL helpers
# -----------------------------
def get_race_id_from_url(url: str) -> str:
    q = parse_qs(urlparse(url).query)
    race_id = (q.get("race_id") or [None])[0]
    if not race_id:
        raise ValueError("URL에 race_id 파라미터가 없습니다.")
    return race_id


def build_url_with_race_id(start_url: str, new_race_id: str) -> str:
    p = urlparse(start_url)
    q = parse_qs(p.query)
    q["race_id"] = [new_race_id]
    new_query = urlencode(q, doseq=True)
    return urlunparse((p.scheme, p.netloc, p.path, p.params, new_query, p.fragment))


# -----------------------------
# Parsing helpers
# -----------------------------
def clean_text_lines(el) -> list[str]:
    raw = el.get_text("\n", strip=True)
    return [ln.strip() for ln in raw.split("\n") if ln.strip()]


def extract_last_token_from_href(href: str):
    if not href:
        return None

    parsed = urlparse(href)
    qs = parse_qs(parsed.query)
    if "id" in qs and qs["id"]:
        return qs["id"][0]

    path = parsed.path or href
    m = re.search(r"([A-Za-z0-9_]+)\D*$", path)
    return m.group(1) if m else None


def parse_sex_age(sex_age_text: str):
    if not sex_age_text:
        return None, None
    s = sex_age_text.strip()
    sex = s[:1] if s else None
    m = re.search(r"(\d+)", s)
    age = int(m.group(1)) if m else None
    return sex, age


def to_int_like(value):
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    try:
        return int(float(s))
    except Exception:
        return None


# -----------------------------
# ✅ Parse ResultPayback Block_Inline (tr[0]=keys, tr[2]=values)  -- KEY_NM 제거 버전
# -----------------------------
def parse_resultpayback_kv(soup: BeautifulSoup, rcno: int) -> list[dict]:
    """
    <div class="ResultPayback Block_Inline"> 하위 table에서
    - tr[0]의 각 셀 텍스트를 KEY
    - tr[2]의 각 셀 텍스트를 VALUE
    로 보고 컬럼 단위로 zip하여 KV row 리스트로 반환.

    반환 row 예:
    {
      "RCNO": 202544100901,
      "KEY": "1200m",
      "VALUE": "..."
    }
    """
    wrap = soup.select_one("div.ResultPayback.Block_Inline")
    if not wrap:
        return []

    table = wrap.select_one("table")
    if not table:
        return []

    trs = table.select("tr")
    if len(trs) < 3:
        return []

    tr_key = trs[0]
    tr_val = trs[2]

    key_cells = tr_key.select("th, td")
    val_cells = tr_val.select("th, td")

    keys = [c.get_text(" ", strip=True) for c in key_cells]
    vals = [c.get_text(" ", strip=True) for c in val_cells]

    out = []
    for k, v in zip(keys, vals):
        k_raw = (k or "").strip()
        v_raw = (v or "").strip()
        if not k_raw and not v_raw:
            continue

        out.append({
            "RCNO": rcno,
            "LAP": k_raw or None,
            "TIME": v_raw or None,
        })

    return out


# -----------------------------
# Parse header (.RaceList_NameBox)
# -----------------------------
def parse_race_header(soup: BeautifulSoup) -> dict:
    el = soup.select_one(".RaceList_NameBox")
    if not el:
        return {}

    lines = clean_text_lines(el)

    out = {
        "RCNAME": None,
        "RCDATE": "2025-10-09",
        "RCDAY": "木",
        "STTIME": None,
        "RCDIST": None,
        "WETR": None,
        "GOING": None,
        "MEET": None,
        "AGECOND": None,
        "RANK": None,
        "DUSU": None,
        "CHAKSUN1": None,
        "CHAKSUN2": None,
        "CHAKSUN3": None,
        "CHAKSUN4": None,
        "CHAKSUN5": None,
    }

    for idx, ln in enumerate(lines):
        if re.match(r"^\d+R$", ln):
            if idx + 1 < len(lines):
                out["RCNAME"] = lines[idx + 1]
            break

    for ln in lines:
        m = re.search(r"(\d{1,2}:\d{2})\s*発走", ln)
        if m:
            out["STTIME"] = m.group(1)
            break

    for ln in lines:
        m = re.search(r"ダ\s*(\d+)\s*m", ln)
        if m:
            out["RCDIST"] = m.group(1)
            break

    for ln in lines:
        if "天候:" in ln:
            out["WETR"] = ln.split("天候:", 1)[1].strip()
            break

    for ln in lines:
        if "馬場:" in ln:
            out["GOING"] = ln.split("馬場:", 1)[1].strip()
            break

    for idx, ln in enumerate(lines):
        if ln.endswith("日目") and idx - 1 >= 0:
            out["MEET"] = lines[idx - 1].strip()
            break

    for ln in lines:
        if " " in ln and ("サラ" in ln or "系" in ln):
            parts = ln.split()
            if len(parts) >= 2:
                out["AGECOND"] = parts[0].strip()
                out["RANK"] = parts[1].strip()
                break

    for ln in lines:
        m = re.match(r"^(\d+)\s*頭$", ln)
        if m:
            out["DUSU"] = m.group(1)
            break

    for ln in lines:
        if ln.startswith("本賞金:"):
            nums = re.findall(r"\d+(?:\.\d+)?", ln)
            keys = ["CHAKSUN1", "CHAKSUN2", "CHAKSUN3", "CHAKSUN4", "CHAKSUN5"]
            for k, v in zip(keys, nums[:5]):
                out[k] = to_int_like(v)
            break

    return out


# -----------------------------
# Parse corner pass from .ResultPayBackRightWrap TABLE #1
# -----------------------------
def parse_corner_pass_table1(soup: BeautifulSoup) -> dict:
    out = {f"CORNER{i}_PASS": None for i in range(1, 5)}

    wrap = soup.select_one(".ResultPayBackRightWrap")
    if not wrap:
        return out

    tables = wrap.select("table")
    if not tables:
        return out

    table1 = tables[0]
    for tr in table1.select("tr"):
        cells = tr.select("th, td")
        texts = [c.get_text(" ", strip=True) for c in cells]

        label_idx = None
        n = None
        for i, t in enumerate(texts):
            m = re.search(r"(\d+)\s*コーナー", t)
            if m:
                label_idx = i
                n = int(m.group(1))
                break

        if label_idx is None or n is None or not (1 <= n <= 5):
            continue

        pass_parts = [t for i, t in enumerate(texts) if i != label_idx and t]
        out[f"CORNER{n}_PASS"] = (" ".join(pass_parts).strip() if pass_parts else None)

    return out


# -----------------------------
# Parse results (.ResultTableWrap)
# -----------------------------
def parse_result_rows(soup: BeautifulSoup) -> list[dict]:
    wrap = soup.select_one(".ResultTableWrap")
    if not wrap:
        return []

    table = wrap.select_one("table")
    if not table:
        return []

    rows = table.select("tr")[1:]  # 헤더 스킵

    results = []
    for tr in rows:
        cells = tr.select("td, th")
        if len(cells) < 14:
            continue

        def cell_text(i1: int) -> str:
            return cells[i1 - 1].get_text(" ", strip=True)

        sex, age = parse_sex_age(cell_text(5))

        hr_cell = cells[4 - 1]
        hr_a = hr_cell.select_one("a")
        hrname = (hr_a.get_text(" ", strip=True) if hr_a else hr_cell.get_text(" ", strip=True))
        hrno = extract_last_token_from_href(hr_a["href"]) if (hr_a and hr_a.has_attr("href")) else None

        jk_cell = cells[7 - 1]
        jk_a = jk_cell.select_one("a")
        jkname = (jk_a.get_text(" ", strip=True) if jk_a else jk_cell.get_text(" ", strip=True))
        jkno = extract_last_token_from_href(jk_a["href"]) if (jk_a and jk_a.has_attr("href")) else None

        tr_cell = cells[13 - 1]
        tr_a = tr_cell.select_one("a")
        trname = (tr_a.get_text(" ", strip=True) if tr_a else tr_cell.get_text(" ", strip=True))
        trno = extract_last_token_from_href(tr_a["href"]) if (tr_a and tr_a.has_attr("href")) else None

        results.append({
            "RK": cell_text(1),
            "WAKU": cell_text(2),
            "CHULNO": cell_text(3),
            "HRNAME": hrname,
            "HRNO": hrno,
            "SEX": sex,
            "AGE": age,
            "WGBUDAM": cell_text(6),
            "JKNAME": jkname,
            "JKNO": jkno,
            "RACE_RCD": cell_text(8),
            "MARGIN": cell_text(9),
            "POPULARITY": cell_text(10),
            "WIN_ODDS": cell_text(11),
            "LAST_3F": cell_text(12),
            "TRNAME": trname,
            "TRNO": trno,
            "RCHR_WEG": cell_text(14),
        })

    return results


# -----------------------------
# Save utilities
# -----------------------------
def ensure_data_dir():
    os.makedirs("data", exist_ok=True)


def append_rows_csv(path: str, rows: list[dict]):
    """
    append + 헤더 확장(새 컬럼 등장 시 재작성)
    """
    if not rows:
        return

    new_keys = set()
    for r in rows:
        new_keys |= set(r.keys())

    if not os.path.exists(path):
        fieldnames = list(rows[0].keys())
        with open(path, "w", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            w.writeheader()
            w.writerows(rows)
        return

    with open(path, "r", newline="", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        old_header = next(reader, [])
    old_keys = set(old_header)

    if new_keys.issubset(old_keys):
        with open(path, "a", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=old_header, extrasaction="ignore")
            w.writerows(rows)
        return

    expanded_header = old_header + [k for k in sorted(new_keys) if k not in old_keys]

    existing_rows = []
    with open(path, "r", newline="", encoding="utf-8-sig") as f:
        dr = csv.DictReader(f)
        for row in dr:
            existing_rows.append(row)

    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=expanded_header, extrasaction="ignore")
        w.writeheader()
        w.writerows(existing_rows)
        w.writerows(rows)


def load_existing_ids(path: str, col: str) -> set[str]:
    ids = set()
    if not os.path.exists(path):
        return ids
    with open(path, "r", newline="", encoding="utf-8-sig") as f:
        r = csv.DictReader(f)
        for row in r:
            v = (row.get(col) or "").strip()
            if v:
                ids.add(v)
    return ids


def save_unique_ids(path: str, col: str, ids: set[str]):
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=[col])
        w.writeheader()
        for v in sorted(ids):
            w.writerow({col: v})


# -----------------------------
# Crawl one race
# -----------------------------
def crawl_one(url: str) -> tuple[list[dict], list[dict], set[str], set[str], set[str]]:
    html = fetch_html(url)
    soup = BeautifulSoup(html, "lxml")

    race_id = get_race_id_from_url(url)

    # ✅ RCNO는 12자리 race_id를 "정수"로 저장
    # (앞자리에 0이 오는 케이스가 없다고 가정. 혹시 있을 수 있으면 str로 두는 게 안전)
    rcno_12 = int(race_id)

    # ✅ payback KV (KEY_NM 없이)
    payback_kv_rows = parse_resultpayback_kv(soup, rcno=rcno_12)

    header = parse_race_header(soup)
    corner_cols = parse_corner_pass_table1(soup)
    results = parse_result_rows(soup)

    # ✅ race_full의 RCNO도 12자리 정수
    common = {"RCNO": rcno_12, **header, **corner_cols}

    merged = []
    hrnos, jknos, trnos = set(), set(), set()

    for r in results:
        merged.append({**common, **r})

        if r.get("HRNO"):
            hrnos.add(str(r["HRNO"]))
        if r.get("JKNO"):
            jknos.add(str(r["JKNO"]))
        if r.get("TRNO"):
            trnos.add(str(r["TRNO"]))

    return merged, payback_kv_rows, hrnos, jknos, trnos


# -----------------------------
# Main: 12 races
# -----------------------------
def main():
    ensure_data_dir()
    #
    # hrno_path = os.path.join("data", "HRNO.csv")
    # jkno_path = os.path.join("data", "JKNO.csv")
    # trno_path = os.path.join("data", "TRNO.csv")
    #
    # all_hrnos = load_existing_ids(hrno_path, "HRNO")
    # all_jknos = load_existing_ids(jkno_path, "JKNO")
    # all_trnos = load_existing_ids(trno_path, "TRNO")

    full_path = os.path.join("data", "race_full.csv")
    payback_kv_path = os.path.join("data", "race_laptime.csv")

    start_race_id = get_race_id_from_url(START_URL)
    width = len(start_race_id)
    start_num = int(start_race_id)

    for i in range(12):
        race_id_i = str(start_num + i).zfill(width)
        url_i = build_url_with_race_id(START_URL, race_id_i)

        print(f"[{i+1:02d}/12] crawl: {race_id_i}")

        try:
            rows, payback_kv_rows, hrnos, jknos, trnos = crawl_one(url_i)
        except Exception as e:
            print(f"  ❌ failed: {race_id_i} | {e}")
            continue

        append_rows_csv(full_path, rows)
        append_rows_csv(payback_kv_path, payback_kv_rows)
        #
        # all_hrnos |= hrnos
        # all_jknos |= jknos
        # all_trnos |= trnos
        #
        # save_unique_ids(hrno_path, "HRNO", all_hrnos)
        # save_unique_ids(jkno_path, "JKNO", all_jknos)
        # save_unique_ids(trno_path, "TRNO", all_trnos)

    print("\n==== DONE ====")
    print(f"Saved race data (append): {full_path}")
    print(f"Saved payback kv (append): {payback_kv_path}")
    # print(f"Unique HRNO count: {len(all_hrnos)}  -> {hrno_path}")
    # print(f"Unique JKNO count: {len(all_jknos)}  -> {jkno_path}")
    # print(f"Unique TRNO count: {len(all_trnos)}  -> {trno_path}")


if __name__ == "__main__":
    main()
