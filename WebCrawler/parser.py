import re
import requests
from bs4 import BeautifulSoup

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0 Safari/537.36"
    ),
    "Referer": "https://race.netkeiba.com/",
}


# ==========================================
# ✨ 신규 추가: 데이터 정제(Cleansing) 함수
# ==========================================
def sanitize_text(val):
    """
    문자열 내부의 제어문자(\r, \n, \t 등)를 제거하고 양끝 공백을 자릅니다.
    숫자나 None 등 문자열이 아닌 타입은 원본 그대로 반환합니다.
    """
    if isinstance(val, str):
        # 1. 줄바꿈, 탭을 임시 공백으로 치환 (단어가 서로 붙는 현상 방지)
        text = re.sub(r'[\r\n\t]+', ' ', val)
        # 2. 보이지 않는 제어문자(ASCII 0~31, 127) 완벽 제거
        text = re.sub(r'[\x00-\x1f\x7f]', '', text)
        # 3. 양끝 공백(전각 공백 포함) 자르기
        return text.strip()
    return val


def parse_cookie_string(raw_cookie: str) -> dict:
    cookie_dict = {}
    if not raw_cookie:
        return cookie_dict
    for c in raw_cookie.split(';'):
        if '=' in c:
            key, value = c.strip().split('=', 1)
            cookie_dict[key] = value
    return cookie_dict


def parse_race_item02(soup: BeautifulSoup, url: str | None = None) -> dict:
    out = {
        "RCNO": None, "RCNAME": None, "RCDATE": None,
        "RCDAY": None, "RANK": None, "STTIME": None,
        "RCDIST": None, "DUSU": None, "MEET": None,
        "AGECOND": None, "CHAKSUN1": None, "CHAKSUN2": None,
        "CHAKSUN3": None, "CHAKSUN4": None, "CHAKSUN5": None,
        "WETR": None, "GOING": None, "TRACK_TYPE": None,
        "DIRECTION": None,
    }

    item02 = soup.select_one(".RaceList_Item02")
    if not item02:
        return out

    # 1. 연도 추출 (URL의 race_id 앞 4자리)
    year = ""
    if url:
        m = re.search(r"race_id=(\d{4})", url)
        if m:
            year = m.group(1)

    # 2. 날짜 및 요일 추출 (.RaceList_Date 내 Active 상태인 태그)
    # 텍스트 형식 예: "3月14日(土)"
    date_tag = soup.select_one(".RaceList_Date dd.Active")
    if date_tag:
        date_text = date_tag.get_text(strip=True)
        m = re.search(r"(\d+)月(\d+)日\((.+)\)", date_text)
        if m:
            month = m.group(1).zfill(2)
            day = m.group(2).zfill(2)
            out["RCDAY"] = m.group(3)
            if year:
                out["RCDATE"] = f"{year}-{month}-{day}"

    race_name_tag = item02.select_one(".RaceName")
    if race_name_tag:
        out["RCNAME"] = race_name_tag.get_text(" ", strip=True)

    data01_tag = item02.select_one(".RaceData01")
    if data01_tag:
        pieces = [s.strip() for s in data01_tag.stripped_strings if s.strip()]
        if len(pieces) == 1 and "/" in pieces[0]:
            pieces = [p.strip() for p in pieces[0].split("/") if p.strip()]

        if len(pieces) >= 1:
            m = re.search(r"(\d{1,2}:\d{2})", pieces[0])
            if m: out["STTIME"] = m.group(1)

        if len(pieces) >= 2:
            m = re.search(r"(\d+)", pieces[1])
            if m: out["RCDIST"] = int(m.group(1))

        joined = " / ".join(pieces)
        m = re.search(r"天候\s*[:：]\s*([^\s/]+)", joined)
        if m: out["WETR"] = m.group(1).strip()

        m = re.search(r"馬場\s*[:：]\s*([^\s/]+)", joined)
        if m: out["GOING"] = m.group(1).strip()

        text_data = data01_tag.get_text(" ", strip=True)
        track_match = re.search(r"([^\s\d]+)\d+m", text_data)
        if track_match: out["TRACK_TYPE"] = track_match.group(1)

        dir_match = re.search(r"\(\s*([^\s\)]+)", text_data)
        if dir_match: out["DIRECTION"] = dir_match.group(1)

    data02_tag = item02.select_one(".RaceData02")
    if data02_tag:
        spans = [sp.get_text(" ", strip=True) for sp in data02_tag.select("span")]
        # 장소 정보(MEET) 추출 로직 강화 (예: "1回 阪神 7日目" -> "阪神")
        if len(spans) >= 2:
            meet_text = spans[1]
            # 공백으로 나누어 경기장 이름에 해당하는 부분을 찾음
            parts = meet_text.split()
            if len(parts) >= 2:
                # 보통 '1回 阪神 7日目' 형식이므로 두 번째 요소가 장소일 가능성이 높음
                out["MEET"] = re.sub(r"\d+.*", "", parts[1]) # 숫자와 그 뒤 내용 제거
            else:
                out["MEET"] = meet_text
        if len(spans) >= 4: out["AGECOND"] = spans[3]
        if len(spans) >= 5:
            rank = spans[4].strip()
            out["RANK"] = rank if rank else None

        if (len(spans) >= 8):
            m = re.search(r"(\d+)", spans[7])
            out["DUSU"] = int(m.group(1)) if m else None

        if len(spans) >= 9:
            last = spans[8]
            cleaned = re.sub(r"^\s*本賞金\s*[:：]\s*", "", last)
            cleaned = re.sub(r"\s*万円\s*$", "", cleaned)
            parts = [p.strip() for p in cleaned.split(",") if p.strip()]

            if len(parts) >= 5:
                out["CHAKSUN1"] = int(parts[0])
                out["CHAKSUN2"] = int(parts[1])
                out["CHAKSUN3"] = int(parts[2])
                out["CHAKSUN4"] = int(parts[3])
                out["CHAKSUN5"] = int(parts[4])

    return out


def parse_race_table01(soup: BeautifulSoup, dusu: int | None = None) -> list[dict]:
    table = soup.select_one(".RaceTable01")
    if not table:
        return []

    rows = table.select("tbody tr")
    horses: list[dict] = []

    for tr in rows:
        tds = tr.find_all("td", recursive=False)
        if not tds or len(tds) < 15:
            continue

        def td_text(i: int) -> str:
            return tds[i].get_text(" ", strip=True)

        rk = td_text(0)
        waku = td_text(1)
        chulno = td_text(2)
        hrname = td_text(3)

        hrno = None
        horse_name_tag = tds[3].select_one(".Horse_Name a")
        if horse_name_tag:
            m = re.search(r"/(\d+)/?$", horse_name_tag.get("href", ""))
            if m: hrno = m.group(1)

        sex_age = td_text(4)
        sex, age = None, None
        m = re.match(r"^\s*([^\d\s])\s*(\d+)\s*$", sex_age)
        if m:
            sex = m.group(1)
            age = int(m.group(2))

        raw_wgbudam = td_text(5)
        m = re.search(r"\d+", raw_wgbudam)
        wgbudam = int(m.group()) if m else None

        jkname = td_text(6)
        jkno = None
        jockey_tag = tds[6].select_one(".Jockey a")
        if jockey_tag:
            m = re.search(r"/(\d+)/?$", jockey_tag.get("href", ""))
            if m: jkno = m.group(1)

        race_rcd = td_text(7)
        margin = td_text(8)
        popularity = td_text(9)
        win_odds = td_text(10)
        last_3f = td_text(11)

        trname = td_text(13)
        trno = None
        trainer_tag = tds[13].select_one(".Trainer a")
        if trainer_tag:
            m = re.search(r"/(\d+)/?$", trainer_tag.get("href", ""))
            if m: trno = m.group(1)

        rchr_weg = td_text(14)

        horse = {
            "RK": rk, "WAKU": waku, "CHULNO": chulno,
            "HRNAME": hrname, "HRNO": hrno, "SEX": sex,
            "AGE": age, "WGBUDAM": wgbudam, "JKNAME": jkname,
            "JKNO": jkno, "RACE_RCD": race_rcd, "MARGIN": margin,
            "POPULARITY": popularity, "WIN_ODDS": win_odds,
            "LAST_3F": last_3f, "TRNAME": trname, "TRNO": trno,
            "RCHR_WEG": rchr_weg,
        }
        horses.append(horse)

        if dusu is not None and len(horses) >= dusu:
            break

    return horses


def parse_premium_lap_summary(soup: BeautifulSoup) -> dict:
    lap_data = {}
    target_table = soup.select_one('#lap_summary')
    if not target_table:
        return lap_data

    MAX_DISTANCE = 20
    horse_rows = target_table.find_all('tr', class_='HorseList')

    for row in horse_rows:
        horse_link = row.select_one('.Horse_Info a')
        horse_id = ""
        if horse_link and 'href' in horse_link.attrs:
            m = re.search(r"/(\d+)/?$", horse_link['href'])
            if m: horse_id = m.group(1)

        if not horse_id:
            continue

        row_data = {f"distance{i}": "" for i in range(1, MAX_DISTANCE + 1)}
        lap_cells = row.select('td[data-laptime]')

        for i, lap in enumerate(lap_cells, start=1):
            if i > MAX_DISTANCE: break
            row_data[f"distance{i}"] = lap.get('data-laptime', "")

        lap_data[horse_id] = row_data

    return lap_data


def parse_race_page_rows(url: str, raw_cookie: str = "") -> list[dict]:
    cookie_dict = parse_cookie_string(raw_cookie)
    res = requests.get(url, headers=HEADERS, cookies=cookie_dict, timeout=15)
    res.encoding = "EUC-JP"
    soup = BeautifulSoup(res.text, "lxml")

    meta = parse_race_item02(soup, url=url)
    m = re.search(r"race_id=(\d+)", url)
    meta["RCNO"] = m.group(1) if m else None

    horses = parse_race_table01(soup, dusu=meta.get("DUSU"))
    premium_laps = parse_premium_lap_summary(soup)

    MAX_DISTANCE = 20
    empty_laps = {f"distance{i}": "" for i in range(1, MAX_DISTANCE + 1)}

    rows = []
    for horse in horses:
        hrno = horse.get("HRNO")
        lap_info = premium_laps.get(hrno, empty_laps)

        # 1. 일단 모든 데이터를 하나의 딕셔너리로 합칩니다.
        raw_row = {**meta, **horse, **lap_info}

        # 2. ✨ 데이터 세탁기(sanitize_text) 가동
        # 합쳐진 딕셔너리의 모든 값을 순회하며 쓰레기 값을 제거합니다.
        cleaned_row = {k: sanitize_text(v) for k, v in raw_row.items()}

        rows.append(cleaned_row)

    return rows