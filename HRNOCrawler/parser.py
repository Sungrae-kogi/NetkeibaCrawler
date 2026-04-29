import re
import asyncio
import aiohttp
import json
from bs4 import BeautifulSoup
from datetime import datetime, timedelta


# ==========================================
# ✨ 신규 추가: 데이터 세탁기
# ==========================================
def sanitize_text(val):
    if isinstance(val, str):
        # 줄바꿈/탭을 공백으로 치환 후, 제어문자 삭제 및 양끝 공백 제거
        text = re.sub(r'[\r\n\t]+', ' ', val)
        text = re.sub(r'[\x00-\x1f\x7f]', '', text)
        return text.strip()
    return val


# ==========================================
# 1. 상수 및 헬퍼 함수
# ==========================================
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Referer": "https://db.netkeiba.com/",
}

BASE_HORSE_URL = "https://db.netkeiba.com/horse/"
PED_AJAX_URL = "https://db.netkeiba.com/horse/ajax_horse_pedigree.html"
RESULTS_AJAX_URL = "https://db.netkeiba.com/horse/ajax_horse_results.html"


def build_horse_url(hrno: str) -> str:
    return f"{BASE_HORSE_URL}{str(hrno).strip()}/"


def _clean_td_value(td) -> str | None:
    if td is None: return None
    v = td.get_text(" ", strip=True)
    if v == "": return None
    if v == "-": return "-"
    return v


def _extract_no(pattern: str, text: str) -> str | None:
    if not text: return None
    m = re.search(pattern, text)
    return m.group(1) if m else None


def _extract_html_from_ajax_json(data: dict) -> str | None:
    if not isinstance(data, dict): return None
    for k in ["html", "data", "result", "body", "content"]:
        v = data.get(k)
        if isinstance(v, str) and "<" in v:
            return v
    return None


def _parse_jp_money(text: str | None) -> str | None:
    if not text: return None
    t = text.strip()
    if t == "-": return "-"
    t = t.replace(",", "").replace(" ", "")
    oku_val = 0
    man_val = 0
    m_oku = re.search(r"(\d+)億", t)
    if m_oku: oku_val = int(m_oku.group(1))

    if "億" in t:
        m_man = re.search(r"億(\d+)万", t)
    else:
        m_man = re.search(r"(\d+)万", t)
    if m_man: man_val = int(m_man.group(1))

    if oku_val > 0 and man_val == 0 and "万" not in t: pass
    if oku_val == 0 and man_val == 0 and "万" not in t and "億" not in t:
        m_pure = re.search(r"(\d+)", t)
        if m_pure: return m_pure.group(1)

    total_man = (oku_val * 10000) + man_val
    return str(total_man)


def _parse_jp_date(date_text: str) -> datetime | None:
    if not date_text: return None
    t = date_text.strip()
    try:
        return datetime.strptime(t, "%Y/%m/%d")
    except Exception:
        pass
    try:
        return datetime.strptime(t, "%Y.%m.%d")
    except Exception:
        pass

    m = re.match(r"^\s*(\d{4})\D+(\d{1,2})\D+(\d{1,2})\s*$", t)
    if m:
        y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
        try:
            return datetime(y, mo, d)
        except Exception:
            return None
    return None


def _parse_prize_to_int(text: str | None) -> int:
    if not text: return 0
    t = text.strip()
    if t == "-" or t == "": return 0
    t = t.replace(",", "")
    m_man = re.search(r"(\d+(?:\.\d+)?)\s*万", t)
    if m_man:
        try:
            return int(float(m_man.group(1)) * 10000)
        except Exception:
            return 0
    m = re.search(r"(\d+)", t)
    if not m: return 0
    try:
        return int(m.group(1))
    except Exception:
        return 0


# ==========================================
# 2. 비동기 통신 함수들
# ==========================================
async def fetch_with_retry(
        session: aiohttp.ClientSession,
        url: str,
        params: dict = None,
        retries: int = 3,
        backoff: float = 1.0
) -> str | None:
    """네트워크 오류 시 재시도 로직을 포함한 비동기 fetch 함수"""
    for i in range(retries):
        try:
            async with session.get(url, params=params, headers=HEADERS, timeout=20) as res:
                if res.status == 200:
                    return await res.text(encoding="euc-jp", errors="replace")
                elif res.status in [429, 500, 502, 503, 504]:
                    # 서버 에러 시 재시도
                    pass
                else:
                    # 404 등 재시도해도 안되는 에러
                    return None
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            if i == retries - 1:
                raise e
        
        await asyncio.sleep(backoff * (2 ** i))
    return None

async def fetch_pedigree_fa_mo(
        hr_no: str,
        session: aiohttp.ClientSession
) -> dict:
    result = {
        "FA_HR_NAME": None, "FA_HR_NO": None,
        "MO_HR_NAME": None, "MO_HR_NO": None,
    }
    if not hr_no: return result
    params = {"input": "UTF-8", "output": "json", "id": hr_no}

    try:
        raw_text = await fetch_with_retry(session, PED_AJAX_URL, params=params)
        if not raw_text: return result
        data = json.loads(raw_text)
    except Exception as e:
        print(f"[WARN] 혈통 AJAX 에러 HRNO={hr_no} / {e}")
        return result

    html = _extract_html_from_ajax_json(data)
    if not html: return result
    soup = BeautifulSoup(html, "lxml")

    fa_a = soup.select_one(".b_ml a")
    if fa_a:
        result["FA_HR_NAME"] = fa_a.get("title") or fa_a.get_text(" ", strip=True)
        href = fa_a.get("href", "")
        result["FA_HR_NO"] = _extract_no(r"/ped/([^/]+)/?", href)

    mo_candidates = []
    for td in soup.select("td.b_fml"):
        a = td.find("a")
        if not a: continue
        href = a.get("href", "")
        ped_no = _extract_no(r"/ped/([^/]+)/?", href)
        ped_name = a.get("title") or a.get_text(" ", strip=True)

        rowspan_raw = td.get("rowspan")
        try:
            rowspan = int(rowspan_raw) if rowspan_raw else 1
        except ValueError:
            rowspan = 1

        mo_candidates.append((rowspan, ped_name, ped_no))

    if mo_candidates:
        mo_candidates.sort(key=lambda x: x[0], reverse=True)
        _, mo_name, mo_no = mo_candidates[0]
        result["MO_HR_NAME"] = mo_name
        result["MO_HR_NO"] = mo_no

    return result


async def fetch_results_counts(
        hr_no: str,
        session: aiohttp.ClientSession
) -> dict:
    result = {
        "RC_CNTT": 0, "ORD1_CNTT": 0, "ORD2_CNTT": 0, "ORD3_CNTT": 0,
        "RC_CNTY": 0, "ORD1_CNTY": 0, "ORD2_CNTY": 0, "ORD3_CNTY": 0,
        "CHAKSUNY": 0, "CHAKSUN_6M": 0,
    }
    if not hr_no: return result
    params = {"input": "UTF-8", "output": "json", "id": hr_no}

    try:
        raw_text = await fetch_with_retry(session, RESULTS_AJAX_URL, params=params)
        if not raw_text: return result
        data = json.loads(raw_text)
    except Exception as e:
        print(f"[WARN] 성적 AJAX 에러 HRNO={hr_no} / {e}")
        return result

    html = _extract_html_from_ajax_json(data)
    if not html: return result
    soup = BeautifulSoup(html, "lxml")

    target_table = None
    for t in soup.find_all("table"):
        if "着順" in t.get_text(" ", strip=True):
            target_table = t
            break

    if target_table is None: target_table = soup.find("table")
    if target_table is None: return result

    rows = target_table.select("tr")
    if len(rows) <= 1: return result

    header_tr = rows[0]
    header_cells = header_tr.find_all(["th", "td"])
    header_texts = [c.get_text(" ", strip=True) for c in header_cells]

    def find_col_idx(keyword: str) -> int | None:
        for i, h in enumerate(header_texts):
            if keyword in h: return i
        return None

    DATE_COL_IDX = find_col_idx("日付")
    PRIZE_COL_IDX = find_col_idx("賞金")
    FINISH_COL_IDX = find_col_idx("着順")
    if FINISH_COL_IDX is None: FINISH_COL_IDX = 11

    data_rows = []
    for tr in rows[1:]:
        tds = tr.find_all("td")
        if not tds: continue
        data_rows.append(tds)

    result["RC_CNTT"] = len(data_rows)
    today = datetime.today()
    cutoff_1y = today - timedelta(days=365)
    cutoff_6m = today - timedelta(days=183)

    for tds in data_rows:
        if FINISH_COL_IDX is not None and len(tds) > FINISH_COL_IDX:
            finish_text = tds[FINISH_COL_IDX].get_text(" ", strip=True)
            m = re.match(r"^(\d+)", finish_text)
            if m:
                fin = int(m.group(1))
                if fin == 1:
                    result["ORD1_CNTT"] += 1
                elif fin == 2:
                    result["ORD2_CNTT"] += 1
                elif fin == 3:
                    result["ORD3_CNTT"] += 1

        if DATE_COL_IDX is None or len(tds) <= DATE_COL_IDX: continue

        date_text = tds[DATE_COL_IDX].get_text(" ", strip=True)
        dt = _parse_jp_date(date_text)
        if dt is None or dt > today: continue

        if dt >= cutoff_1y:
            result["RC_CNTY"] += 1
            if len(tds) > FINISH_COL_IDX:
                finish_text = tds[FINISH_COL_IDX].get_text(" ", strip=True)
                m = re.match(r"^(\d+)", finish_text)
                if m:
                    fin = int(m.group(1))
                    if fin == 1:
                        result["ORD1_CNTY"] += 1
                    elif fin == 2:
                        result["ORD2_CNTY"] += 1
                    elif fin == 3:
                        result["ORD3_CNTY"] += 1

            if PRIZE_COL_IDX is not None and len(tds) > PRIZE_COL_IDX:
                prize_text = tds[PRIZE_COL_IDX].get_text(" ", strip=True)
                result["CHAKSUNY"] += _parse_prize_to_int(prize_text)

        if dt >= cutoff_6m:
            if PRIZE_COL_IDX is not None and len(tds) > PRIZE_COL_IDX:
                prize_text = tds[PRIZE_COL_IDX].get_text(" ", strip=True)
                result["CHAKSUN_6M"] += _parse_prize_to_int(prize_text)

    return result


# ==========================================
# 3. 메인 파서 함수
# ==========================================
async def parse_horse_page(
        url: str,
        hr_no: str,
        session: aiohttp.ClientSession
) -> dict:
    out = {
        "HR_NO": hr_no,
        "HR_NAME": None, "ENG_HRNM": None, "SEX": None, "AGE": None, "BIRTHDAY": None,
        "TR_NAME": None, "TR_NO": None, "OW_NAME": None, "OW_NO": None,
        "BREEDER": None, "BRED_REGION": None, "HR_LAST_AMT": None,
        "CHAKSUNT_JRA": None, "CHAKSUNT_NAR": None, "CAREER_TOTAL": None,
        "MAIN_WINS": None, "RELATED_HORSES": None,
        "FA_HR_NAME": None, "FA_HR_NO": None, "MO_HR_NAME": None, "MO_HR_NO": None,
        "RC_CNTT": None, "ORD1_CNTT": None, "ORD2_CNTT": None, "ORD3_CNTT": None,
        "RC_CNTY": None, "ORD1_CNTY": None, "ORD2_CNTY": None, "ORD3_CNTY": None,
        "CHAKSUNY": None, "CHAKSUN_6M": None,
    }

    html = await fetch_with_retry(session, url)
    if not html: return out

    soup = BeautifulSoup(html, "lxml")

    title_tag = soup.select_one(".horse_title")
    if title_tag:
        h1 = title_tag.find("h1")
        if h1:
            out["HR_NAME"] = h1.get_text(" ", strip=True).split()[0]
        else:
            out["HR_NAME"] = title_tag.get_text(" ", strip=True).split()[0]
            
        eng_tag = title_tag.select_one(".eng_name a") or title_tag.select_one(".eng_name")
        if eng_tag:
            out["ENG_HRNM"] = eng_tag.get_text(" ", strip=True)

    txt01_tag = soup.select_one(".txt_01")
    if txt01_tag:
        txt = txt01_tag.get_text(" ", strip=True)
        m2 = re.search(r"(牡|牝|セ)\s*(\d+)?", txt)
        if m2:
            out["SEX"] = m2.group(1)
            out["AGE"] = m2.group(2)

    prof_area = soup.select_one("div.db_prof_area_02") or soup.select_one("div.db_prof")
    if prof_area:
        prof_table = prof_area.find("table")
        if prof_table:
            rows = prof_table.select("tr")
            for tr in rows:
                th = tr.find("th")
                td = tr.find("td")
                if not th or not td: continue

                key = th.get_text(" ", strip=True)
                td_text = _clean_td_value(td)

                if "生年月日" in key:
                    if td_text:
                        # 방어 코드: YYYY年M月D日 구조인지 검증
                        if re.match(r"^\d{4}年\d{1,2}月\d{1,2}日$", td_text):
                            out["BIRTHDAY"] = td_text
                        else:
                            print(f"[WARN] 생년월일 포맷 이상 감지 (HRNO={hr_no}): '{td_text}'")
                            # 데이터 유실 방지를 위해 원본 텍스트를 유지하거나, 추가적인 처리를 할 수 있습니다.
                            out["BIRTHDAY"] = td_text
                    else:
                        out["BIRTHDAY"] = None
                elif "調教師" in key:
                    a = td.find("a")
                    if a:
                        out["TR_NAME"] = a.get("title") or _clean_td_value(a)
                        out["TR_NO"] = _extract_no(r"/trainer/([^/]+)/?", a.get("href", ""))
                    else:
                        out["TR_NAME"] = td_text
                elif "馬主" in key:
                    a = td.find("a")
                    if a:
                        out["OW_NAME"] = a.get("title") or _clean_td_value(a)
                        out["OW_NO"] = _extract_no(r"/owner/([^/]+)/?", a.get("href", ""))
                    else:
                        out["OW_NAME"] = td_text
                elif "産地" in key:
                    out["BRED_REGION"] = td_text
                elif "生産者" in key:
                    out["BREEDER"] = td_text
                elif "獲得賞金 (中央)" in key:
                    out["CHAKSUNT_JRA"] = _parse_jp_money(td_text)
                elif ("獲得賞金 (地方)" in key) or ("獲得賞金(地方)" in key) or ("獲得賞金（地方）" in key):
                    out["CHAKSUNT_NAR"] = _parse_jp_money(td_text)
                elif "通算成績" in key:
                    out["CAREER_TOTAL"] = td_text
                elif "主な勝鞍" in key:
                    out["MAIN_WINS"] = td_text
                elif "近親馬" in key:
                    out["RELATED_HORSES"] = td_text
                elif "セリ取引価格" in key:
                    out["HR_LAST_AMT"] = _parse_jp_money(td_text)

    ped_task = fetch_pedigree_fa_mo(hr_no, session)
    counts_task = fetch_results_counts(hr_no, session)
    ped, counts = await asyncio.gather(ped_task, counts_task)

    out["FA_HR_NAME"] = ped["FA_HR_NAME"]
    out["FA_HR_NO"] = ped["FA_HR_NO"]
    out["MO_HR_NAME"] = ped["MO_HR_NAME"]
    out["MO_HR_NO"] = ped["MO_HR_NO"]

    out["RC_CNTT"] = counts["RC_CNTT"]
    out["ORD1_CNTT"] = counts["ORD1_CNTT"]
    out["ORD2_CNTT"] = counts["ORD2_CNTT"]
    out["ORD3_CNTT"] = counts["ORD3_CNTT"]
    out["RC_CNTY"] = counts["RC_CNTY"]
    out["ORD1_CNTY"] = counts["ORD1_CNTY"]
    out["ORD2_CNTY"] = counts["ORD2_CNTY"]
    out["ORD3_CNTY"] = counts["ORD3_CNTY"]
    out["CHAKSUNY"] = counts["CHAKSUNY"]
    out["CHAKSUN_6M"] = counts["CHAKSUN_6M"]

    # ✨ 최종 반환 직전, 딕셔너리의 모든 값을 세탁기에 돌립니다.
    cleaned_out = {k: sanitize_text(v) for k, v in out.items()}
    return cleaned_out