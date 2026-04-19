# parser.py
from __future__ import annotations

import re
import datetime as dt
from bs4 import BeautifulSoup

SPACE_RE = re.compile(r"\s+")
BIRTH_RE = re.compile(r"(\d{4})[\/\-\.](\d{1,2})[\/\-\.](\d{1,2})")


# ✅ Table1: HEIGHTWEIGHT -> HEIGHT, WEIGHT 로 분리
TABLE1_COLS = [
    "HEIGHT",           # 신장
    "WEIGHT",           # 체중
    "HOMETOWN",         # 출신지
    "BLOODTYPE",        # 혈액형
    "DEBUT",            # 데뷔년
    "WINS_THIS_YEAR",   # 올해 승리 수
    "WINS_TOTAL",       # 통산 승리 수
    "PRIZE_THIS_YEAR",  # 올해 획득 상금
    "PRIZE_TOTAL",      # 통산 획득 상금
    "GI_WINS",          # GI 승수
    "GRADED_WINS_1",    # (원문: GRADED_WINS) 중상 승리 수
]

TABLE2_COLS = [
    "FIRST_DEBUT",      # 첫 출주
    "FIRST_WIN_DATE",   # 첫승리
    "GRADED_STARTS",    # 초중상 출주
    "GRADED_WINS_2",    # (원문: GRADED_WINS) 초중상 승리
    "FIRST_GI_START",   # 첫G1 출주
    "FIRST_GI_WIN",     # 첫G1 승리
]

# ✅ result.html에서 추가할 컬럼
RESULT_STATS_COLS = [
    # 통산(年度=通算)
    "RCCNTT",
    "ORD1CNTT",
    "ORD2CNTT",
    "ORD3CNTT",
    "WINRATET",
    "QNLRATET",
    # 2026(年度=2026)
    "ORD1CNTY",
    "ORD2CNTY",
    "ORD3CNTY",
    "WINRATEY",
    "QNLRATEY",
    "RCCNTY",
]


def _clean(s: str | None) -> str:
    if not s:
        return ""
    return SPACE_RE.sub(" ", s).strip()


def _extract_tr_pairs(table) -> list[tuple[str, str]]:
    """
    table 내 모든 tr에서 (th_text, td_text) 리스트 반환
    """
    pairs: list[tuple[str, str]] = []
    for tr in table.select("tr"):
        th = tr.find("th")
        td = tr.find("td")
        if not th or not td:
            continue
        pairs.append((_clean(th.get_text(" ", strip=True)), _clean(td.get_text(" ", strip=True))))
    return pairs


def _extract_number(s: str) -> str:
    """
    문자열에서 숫자(정수/소수) 첫 번째 매치만 추출. 없으면 "".
    """
    m = re.search(r"\d+(?:\.\d+)?", s or "")
    return m.group(0) if m else ""


def _split_height_weight_numeric(v: str) -> tuple[str, str]:
    """
    예: '164cm/53kg' -> ('164', '53')
        '164 / 53', '身長164cm/体重53kg' 등 변형도 대응
    """
    v = _clean(v)
    if not v:
        return "", ""

    v = v.replace("／", "/")
    parts = [p.strip() for p in v.split("/") if p.strip()]

    height = _extract_number(parts[0]) if len(parts) >= 1 else ""
    weight = _extract_number(parts[1]) if len(parts) >= 2 else ""
    return height, weight


def _parse_birthday_from_name_p(soup: BeautifulSoup) -> dt.date | None:
    """
    .Name p 텍스트에서 YYYY/MM/DD 형태(또는 YYYY-MM-DD, YYYY.MM.DD) 추출 후 date 변환
    """
    p_el = soup.select_one(".Name p")
    txt = _clean(p_el.get_text(" ", strip=True) if p_el else "")
    if not txt:
        return None

    m = BIRTH_RE.search(txt)
    if not m:
        return None

    y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
    try:
        return dt.date(y, mo, d)
    except ValueError:
        return None


def _calc_age(birth: dt.date, today: dt.date) -> int:
    """
    만 나이 계산
    """
    age = today.year - birth.year
    if (today.month, today.day) < (birth.month, birth.day):
        age -= 1
    return max(age, 0)


def parse_jockey_profile(html: str, jkno: str, debug: bool = False) -> dict[str, str]:
    """
    /jockey/{JKNO}/ 페이지 HTML에서
    - .Name h1 -> JKNAME
    - .Name p  -> BIRTHDAY(YYYY-MM-DD), AGE(만 나이)
    - .ProfileDataTable 내 테이블 2개 tr을 순서대로 컬럼에 매핑
    """
    soup = BeautifulSoup(html, "lxml")

    # JKNAME: .Name h1
    name_el = soup.select_one(".Name h1")
    jkname = _clean(name_el.get_text(" ", strip=True) if name_el else "")

    # BIRTHDAY, AGE: .Name p
    birth_date = _parse_birthday_from_name_p(soup)
    today = dt.date.today()
    birthday_str = birth_date.isoformat() if birth_date else ""
    age_str = str(_calc_age(birth_date, today)) if birth_date else ""

    row: dict[str, str] = {
        "JKNO": jkno,
        "JKNAME": jkname,
        "BIRTHDAY": birthday_str,
        "AGE": age_str,
    }

    profile = soup.select_one(".ProfileDataTable")
    if profile is None:
        if debug:
            print("[DEBUG] .ProfileDataTable NOT FOUND")
            print(
                f"[DEBUG] JKNO={jkno!r} JKNAME={jkname!r} BIRTHDAY={birthday_str!r} AGE={age_str!r}"
            )
        for col in TABLE1_COLS + TABLE2_COLS:
            row[col] = ""
        return row

    tables = profile.select("table")
    if debug:
        print(f"[DEBUG] found tables in .ProfileDataTable: {len(tables)}")
        print(
            f"[DEBUG] JKNO={jkno!r} JKNAME={jkname!r} BIRTHDAY={birthday_str!r} AGE={age_str!r} (today={today.isoformat()})"
        )

    t1 = tables[0] if len(tables) >= 1 else None
    t2 = tables[1] if len(tables) >= 2 else None

    # --- Table 1 ---
    if t1 is not None:
        pairs1 = _extract_tr_pairs(t1)

        if debug:
            print(f"[DEBUG] table1 tr(th:td) count={len(pairs1)}")
            for i, (k, v) in enumerate(pairs1[:20]):
                print(f"  [t1 row{i}] {k!r} : {v!r}")

        hw_value = pairs1[0][1] if len(pairs1) >= 1 else ""
        height, weight = _split_height_weight_numeric(hw_value)
        row["HEIGHT"] = height
        row["WEIGHT"] = weight

        remaining_cols = TABLE1_COLS[2:]  # HEIGHT, WEIGHT 제외

        tr_i = 1
        col_i = 0

        while col_i < len(remaining_cols):
            col = remaining_cols[col_i]
            value = pairs1[tr_i][1] if tr_i < len(pairs1) else ""

            # HOMETOWN에 "출신지/혈액형" 형태로 오는 경우 분리
            if col == "HOMETOWN" and value:
                norm = value.replace("／", "/").strip()

                if "/" in norm:
                    parts = [p.strip() for p in norm.split("/") if p.strip()]
                    row["HOMETOWN"] = parts[0] if len(parts) >= 1 else ""
                    row["BLOODTYPE"] = parts[1] if len(parts) >= 2 else ""

                    # 다음 컬럼이 BLOODTYPE이면 스킵
                    if col_i + 1 < len(remaining_cols) and remaining_cols[col_i + 1] == "BLOODTYPE":
                        col_i += 1
                else:
                    row["HOMETOWN"] = norm
                    row["BLOODTYPE"] = ""

                tr_i += 1
                col_i += 1
                continue

            # BLOODTYPE은 HOMETOWN에서 못 채운 경우 빈값 유지
            if col == "BLOODTYPE":
                row["BLOODTYPE"] = row.get("BLOODTYPE", "")
                col_i += 1
                continue

            row[col] = value
            tr_i += 1
            col_i += 1

        for col in remaining_cols:
            if col not in row:
                row[col] = ""

    else:
        for col in TABLE1_COLS:
            row[col] = ""

    # --- Table 2 ---
    if t2 is not None:
        pairs2 = _extract_tr_pairs(t2)

        if debug:
            print(f"[DEBUG] table2 tr(th:td) count={len(pairs2)}")
            for i, (k, v) in enumerate(pairs2[:20]):
                print(f"  [t2 row{i}] {k!r} : {v!r}")

        for idx, col in enumerate(TABLE2_COLS):
            row[col] = pairs2[idx][1] if idx < len(pairs2) else ""
    else:
        for col in TABLE2_COLS:
            row[col] = ""

    if debug:
        print("[DEBUG] final mapped row keys/values:")
        for k in ["JKNO", "JKNAME", "BIRTHDAY", "AGE"] + TABLE1_COLS + TABLE2_COLS:
            print(f"  - {k}: {row.get(k, '')!r}")

    return row


# ===============================
# result.html 파서 (요청 매핑)
# ===============================
def _to_int_str(s: str) -> str:
    """
    숫자 문자열에서 콤마 제거 후 정수만 추출해 문자열로 반환.
    (비어있으면 "")
    """
    t = _clean(s).replace(",", "")
    m = re.search(r"^\d+", t)
    return m.group(0) if m else ""


def _to_float_str_percent_cell(s: str) -> str:
    """
    셀 텍스트에서 % 제거 후 float 숫자만 추출해서 문자열로 반환.
    예:
      "12.3%" -> "12.3"
      " 0% "  -> "0.0"
      "-" / "" / "—" -> ""
    """
    t = _clean(s)
    if not t or t in ("-", "—"):
        return ""

    t = t.replace("%", "").replace(",", "").strip()

    m = re.search(r"\d+(?:\.\d+)?", t)
    if not m:
        return ""

    try:
        return str(float(m.group(0)))
    except Exception:
        return m.group(0)


def parse_jockey_result_stats(html: str, jkno: str, debug: bool = False) -> dict[str, str]:
    """
    https://db.netkeiba.com/jockey/result.html?id=JKNO
    .contents_liquid 아래 테이블 중 年度가 있는 테이블에서
    - 通算 행: 17번째 td -> WINRATET, 19번째 td -> QNLRATET
    - 2026 행: 17번째 td -> WINRATEY, 19번째 td -> QNLRATEY
    - RCCNTY: 2026행의 7,9,11,13,15번째 td 합
    (모두 % 제거 후 float/정수 숫자만)
    """
    out = {k: "" for k in RESULT_STATS_COLS}

    soup = BeautifulSoup(html, "lxml")
    root = soup.select_one("#contents_liquid")
    if root is None:
        if debug:
            print(f"[DEBUG][result] .contents_liquid NOT FOUND jkno={jkno}")
        return out

    tables = root.select("table")
    if not tables:
        if debug:
            print(f"[DEBUG][result] no tables under .contents_liquid jkno={jkno}")
        return out

    # 1) 年度 컬럼이 있는 테이블 하나 선택
    target = None
    idx_year = None
    headers: list[str] = []

    for t in tables:
        trs = t.select("tr")
        if not trs:
            continue

        header_cells = trs[0].find_all(["th", "td"])
        headers = [_clean(c.get_text(" ", strip=True)) for c in header_cells]

        for i, h in enumerate(headers):
            if "年度" in h:
                target = t
                idx_year = i
                break

        if target is not None:
            break

    if target is None or idx_year is None:
        if debug:
            print(f"[DEBUG][result] table with 年度 header NOT FOUND jkno={jkno}")
        return out

    def find_idx_contains(label: str) -> int | None:
        for i, h in enumerate(headers):
            if label in h:
                return i
        return None

    idx_ord1 = find_idx_contains("1着")
    idx_ord2 = find_idx_contains("2着")
    idx_ord3 = find_idx_contains("3着")

    # 2) 年度 값으로 행 찾기
    data_trs = target.select("tr")[1:]

    def get_cells_by_year(year_value: str):
        for tr in data_trs:
            cells = tr.find_all(["th", "td"])
            if len(cells) <= idx_year:
                continue
            y = _clean(cells[idx_year].get_text(" ", strip=True))
            if y == year_value:
                return cells
        return None

    tds_total = get_cells_by_year("通算")
    tds_2026 = get_cells_by_year("2026")

    if debug:
        print(f"[DEBUG][result] idx_year={idx_year} idx_ord1={idx_ord1} idx_ord2={idx_ord2}")
        print(f"[DEBUG][result] found 通算 row={bool(tds_total)} / 2026 row={bool(tds_2026)}")
        if tds_total:
            print(f"[DEBUG][result] 通算 cols={len(tds_total)}")
        if tds_2026:
            print(f"[DEBUG][result] 2026 cols={len(tds_2026)}")

    # ---- 통산(通算) ----
    if tds_total:
        if idx_ord1 is not None and len(tds_total) > idx_ord1:
            out["ORD1CNTT"] = _to_int_str(tds_total[idx_ord1].get_text(" ", strip=True))
        if idx_ord2 is not None and len(tds_total) > idx_ord2:
            out["ORD2CNTT"] = _to_int_str(tds_total[idx_ord2].get_text(" ", strip=True))
        if idx_ord3 is not None and len(tds_total) > idx_ord3:
            out["ORD3CNTT"] = _to_int_str(
                tds_total[idx_ord3].get_text(" ", strip=True)
            )

        # ✅ 17번째 td (1-based) = index 16
        if len(tds_total) > 16:
            out["WINRATET"] = _to_float_str_percent_cell(tds_total[16].get_text(" ", strip=True))

        # ✅ 19번째 td (1-based) = index 18
        if len(tds_total) > 18:
            out["QNLRATET"] = _to_float_str_percent_cell(tds_total[18].get_text(" ", strip=True))

        # ✅ RCCNTT: 2026행의 7,9,11,13,15번째 td 합 (1-based)
        # -> index 6,8,10,12,14 (0-based)
        sum_idxs = [6, 8, 10, 12, 14]
        total_runs = 0
        for j in sum_idxs:
            if len(tds_total) <= j:
                continue
            v = _to_int_str(tds_total[j].get_text(" ", strip=True))
            if v:
                total_runs += int(v)
        out["RCCNTT"] = str(total_runs) if total_runs else ""

    # ---- 2026 ----
    if tds_2026:
        if idx_ord1 is not None and len(tds_2026) > idx_ord1:
            out["ORD1CNTY"] = _to_int_str(tds_2026[idx_ord1].get_text(" ", strip=True))
        if idx_ord2 is not None and len(tds_2026) > idx_ord2:
            out["ORD2CNTY"] = _to_int_str(tds_2026[idx_ord2].get_text(" ", strip=True))

        if idx_ord3 is not None and len(tds_2026) > idx_ord3:
            out["ORD3CNTY"] = _to_int_str(
                tds_2026[idx_ord3].get_text(" ", strip=True)
            )

        # ✅ 17번째 td (1-based) = index 16
        if len(tds_2026) > 16:
            out["WINRATEY"] = _to_float_str_percent_cell(tds_2026[16].get_text(" ", strip=True))

        # ✅ 19번째 td (1-based) = index 18
        if len(tds_2026) > 18:
            out["QNLRATEY"] = _to_float_str_percent_cell(tds_2026[18].get_text(" ", strip=True))

        # ✅ RCCNTY: 2026행의 7,9,11,13,15번째 td 합 (1-based)
        # -> index 6,8,10,12,14 (0-based)
        sum_idxs = [6, 8, 10, 12, 14]
        total_runs = 0
        for j in sum_idxs:
            if len(tds_2026) <= j:
                continue
            v = _to_int_str(tds_2026[j].get_text(" ", strip=True))
            if v:
                total_runs += int(v)
        out["RCCNTY"] = str(total_runs) if total_runs else ""

    if debug:
        print("[DEBUG][result] mapped result stats:")
        for k in RESULT_STATS_COLS:
            print(f"  - {k}: {out.get(k, '')!r}")

        # 지정한 td 위치 확인용
        if tds_total and len(tds_total) > 18:
            print("[DEBUG][result] 通算 td[16]/td[18] raw =",
                  _clean(tds_total[16].get_text(" ", strip=True)),
                  _clean(tds_total[18].get_text(" ", strip=True)))
        if tds_2026 and len(tds_2026) > 18:
            print("[DEBUG][result] 2026 td[16]/td[18] raw =",
                  _clean(tds_2026[16].get_text(" ", strip=True)),
                  _clean(tds_2026[18].get_text(" ", strip=True)))
            # RCCNTY 합산 대상 td 확인
            raw_runs = []
            for j in [6, 8, 10, 12, 14]:
                raw_runs.append(_clean(tds_2026[j].get_text(" ", strip=True)) if len(tds_2026) > j else "N/A")
            print("[DEBUG][result] 2026 RCCNTY source tds =", raw_runs)

    return out
