import re
from typing import Dict, Tuple, Set
from bs4 import BeautifulSoup

def parse_api_race_plan(soup: BeautifulSoup, url: str) -> Dict:
    out = {
        "RCCRS_NM": None,
        "RACE_DT": None,
        "RACE_DY_CNT": None,
        "RACE_DOTW": None,
        "RACE_NO": None,
        "RCGRD": None,
        "RACE_CLAS": None,
        "RACE_NM": None,
        "RACE_DS": None,
        "PTIN_NHR": None,
        "CNDTS_AG": None,
        "CNDTS_GNDR": None,
        "CNDTS_BURD_WGT": None,
        "RPM_FPLC": None,
        "RPM_SPLC": None,
        "RPM_TPLC": None,
        "RPM_FOPLC": None,
        "RPM_FVPLC": None,
        "ADMNY_FPLC": None,
        "ADMNY_SPLC": None,
        "ADMNY_TPLC": None,
        "STRT_PARG_TM": None,
        "STRT_TM": None,
        "WETR": None,
        "GOING": None,
        "RCTYPE": None,
        "RCDIRECTION": None,
    }

    # 1. URL parsing (RACE_DT year, RACE_NO)
    year = ""
    m_url = re.search(r"race_id=(\d{4})(\d{4})(\d{2})(\d{2})", url)
    if m_url:
        year = m_url.group(1)
        out["RACE_NO"] = int(m_url.group(4))

    # 2. Date Tag (.RaceList_Date dd.Active)
    date_tag = soup.select_one(".RaceList_Date dd.Active")
    if date_tag:
        date_text = date_tag.get_text(strip=True)
        m_date = re.search(r"(\d+)月(\d+)日\s*\((.+)\)", date_text)
        if m_date:
            month = m_date.group(1).zfill(2)
            day = m_date.group(2).zfill(2)
            out["RACE_DOTW"] = m_date.group(3)
            if year:
                out["RACE_DT"] = int(f"{year}{month}{day}")

    # 3. Race Name (.RaceName)
    race_name_tag = soup.select_one(".RaceList_Item02 .RaceName")
    if race_name_tag:
         out["RACE_NM"] = race_name_tag.get_text(" ", strip=True)

    # 4. Data01 (.RaceData01)
    data01_tag = soup.select_one(".RaceList_Item02 .RaceData01")
    if data01_tag:
        text_data01 = data01_tag.get_text(" ", strip=True)
        
        # STRT_PARG_TM
        m_time = re.search(r"(\d{1,2}:\d{2})発走", text_data01)
        if m_time:
            out["STRT_PARG_TM"] = m_time.group(1)
            out["STRT_TM"] = out["STRT_PARG_TM"]
            
        # RCTYPE & RACE_DS
        m_track = re.search(r"([ダ芝障])(\d+)m", text_data01)
        if m_track:
            out["RCTYPE"] = m_track.group(1)
            out["RACE_DS"] = int(m_track.group(2))
            
        # RCDIRECTION
        m_dir = re.search(r"m\s*\((.*?)\)", text_data01)
        if m_dir:
            dir_text = m_dir.group(1)
            if "右" in dir_text:
                out["RCDIRECTION"] = "右"
            elif "左" in dir_text:
                out["RCDIRECTION"] = "左"
            elif "直" in dir_text:
                out["RCDIRECTION"] = "直線"
            else:
                out["RCDIRECTION"] = dir_text.strip()

    # 5. Data02 (.RaceData02)
    data02_tag = soup.select_one(".RaceList_Item02 .RaceData02")
    if data02_tag:
        spans = [sp.get_text(" ", strip=True) for sp in data02_tag.select("span")]
        
        if len(spans) >= 2:
            meet_text = spans[1]
            m_meet = re.search(r"([^\s\d回]+)", meet_text)
            if m_meet:
                out["RCCRS_NM"] = m_meet.group(1)
            else:
                out["RCCRS_NM"] = meet_text.split()[-1] if ' ' in meet_text else meet_text

        if len(spans) >= 4:
            out["CNDTS_AG"] = spans[3].strip()
        if len(spans) >= 5:
            rcgrd = spans[4].strip()
            out["RCGRD"] = rcgrd if rcgrd else None
            out["RACE_CLAS"] = out["RCGRD"]
            
        # 성별 조건 스캔 (없으면 기본값 '混')
        gndr_val = "混"
        for sp in spans:
            if "牝" in sp or "混合" in sp or "牡" in sp:
                m_gndr = re.search(r"([牝牡混合]+)", sp)
                if m_gndr:
                    gndr_val = m_gndr.group(1)
                    break
        out["CNDTS_GNDR"] = gndr_val
            
        for sp in spans:
            # PTIN_NHR
            m_nhr = re.search(r"(\d+)頭", sp)
            if m_nhr:
                out["PTIN_NHR"] = int(m_nhr.group(1))
                
            # Prize
            if "本賞金:" in sp or "本賞金：" in sp:
                cleaned = re.sub(r"^\s*本賞金\s*[:：]\s*", "", sp)
                cleaned = re.sub(r"\s*万円\s*$", "", cleaned)
                parts = [p.strip() for p in cleaned.split(",") if p.strip()]
                if len(parts) >= 1: out["RPM_FPLC"] = parts[0]
                if len(parts) >= 2: out["RPM_SPLC"] = parts[1]
                if len(parts) >= 3: out["RPM_TPLC"] = parts[2]
                if len(parts) >= 4: out["RPM_FOPLC"] = parts[3]
                if len(parts) >= 5: out["RPM_FVPLC"] = parts[4]
                
    return out

def parse_pks(soup: BeautifulSoup) -> Tuple[Set[str], Set[str], Set[str]]:
    hrnos, jknos, trnos = set(), set(), set()
    table = soup.select_one(".RaceTable01")
    if not table:
        return hrnos, jknos, trnos
        
    rows = table.select("tr.HorseList")
    for row in rows:
        # Horse
        hr_tag = row.select_one("td.HorseInfo a")
        if hr_tag and 'href' in hr_tag.attrs:
            m = re.search(r"horse/(20\d+)", hr_tag['href'])
            if not m:
                m = re.search(r"horse/(\d+)", hr_tag['href'])
            if m: hrnos.add(m.group(1))
            
        # Jockey
        jk_tag = row.select_one("td.Jockey a")
        if jk_tag and 'href' in jk_tag.attrs:
            m = re.search(r"/jockey/.*?(\d{4,5})", jk_tag['href'])
            if m: jknos.add(m.group(1))
            
        # Trainer
        tr_tag = row.select_one("td.Trainer a")
        if tr_tag and 'href' in tr_tag.attrs:
            m = re.search(r"/trainer/.*?(\d{4,5})", tr_tag['href'])
            if m: trnos.add(m.group(1))

    return hrnos, jknos, trnos
