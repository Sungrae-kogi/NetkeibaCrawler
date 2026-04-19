import re
from typing import Dict, List
from bs4 import BeautifulSoup

def parse_api_entry_sheet_2(soup: BeautifulSoup, url: str) -> List[Dict]:
    # 1. Parse common race metadata
    year = ""
    rcno = None
    m_url = re.search(r"race_id=(\d{4})(\d{4})(\d{2})(\d{2})", url)
    if m_url:
        year = m_url.group(1)
        rcno = int(m_url.group(4))

    base_out = {
        "MEET": None,
        "RCDATE": None,
        "RCDAY": None,
        "RCNO": rcno,
        "ILSU": None, # Null
        "RCDIST": None,
        "DUSU": None,
        "RANK": None,
        "AGECOND": None,
        "STTIME": None,
        "BUDAM": None,
        "RCNAME": None,
        "CHAKSUN1": None,
        "CHAKSUN2": None,
        "CHAKSUN3": None,
        "CHAKSUN4": None,
        "CHAKSUN5": None,
    }

    # Date Tag (.RaceList_Date dd.Active)
    date_tag = soup.select_one(".RaceList_Date dd.Active")
    if date_tag:
        date_text = date_tag.get_text(strip=True)
        m_date = re.search(r"(\d+)月(\d+)日\s*\((.+)\)", date_text)
        if m_date:
            month = m_date.group(1).zfill(2)
            day = m_date.group(2).zfill(2)
            base_out["RCDAY"] = m_date.group(3)
            if year:
                base_out["RCDATE"] = int(f"{year}{month}{day}")

    # Race Name (.RaceName)
    race_name_tag = soup.select_one(".RaceList_Item02 .RaceName")
    if race_name_tag:
         base_out["RCNAME"] = race_name_tag.get_text(" ", strip=True)

    # Data01 (.RaceData01)
    data01_tag = soup.select_one(".RaceList_Item02 .RaceData01")
    if data01_tag:
        text_data01 = data01_tag.get_text(" ", strip=True)
        m_time = re.search(r"(\d{1,2}:\d{2})発走", text_data01)
        if m_time:
            base_out["STTIME"] = m_time.group(1)
            
        m_track = re.search(r"[ダ芝障](\d+)m", text_data01)
        if m_track:
            base_out["RCDIST"] = int(m_track.group(1))

    # Data02 (.RaceData02)
    data02_tag = soup.select_one(".RaceList_Item02 .RaceData02")
    if data02_tag:
        spans = [sp.get_text(" ", strip=True) for sp in data02_tag.select("span")]
        if len(spans) >= 2:
            meet_text = spans[1]
            m_meet = re.search(r"([^\s\d回]+)", meet_text)
            if m_meet: base_out["MEET"] = m_meet.group(1)
            else: base_out["MEET"] = meet_text.split()[-1] if ' ' in meet_text else meet_text

        if len(spans) >= 4:
            base_out["AGECOND"] = spans[3].strip()
        if len(spans) >= 5:
            base_out["RANK"] = spans[4].strip() if spans[4].strip() else None
            
        for sp in spans:
            m_nhr = re.search(r"(\d+)頭", sp)
            if m_nhr: base_out["DUSU"] = int(m_nhr.group(1))
            
            if "馬齢" in sp or "定量" in sp or "ハンデ" in sp or "別定" in sp:
                base_out["BUDAM"] = sp.strip()
                
            if "本賞金" in sp or "本賞金：" in sp:
                cleaned = re.sub(r"^\s*本賞金\s*[:：]\s*", "", sp)
                cleaned = re.sub(r"\s*만원\s*$", "", cleaned)
                cleaned = re.sub(r"\s*万円\s*$", "", cleaned)
                parts = [p.strip() for p in cleaned.split(",") if p.strip()]
                if len(parts) >= 1: base_out["CHAKSUN1"] = parts[0]
                if len(parts) >= 2: base_out["CHAKSUN2"] = parts[1]
                if len(parts) >= 3: base_out["CHAKSUN3"] = parts[2]
                if len(parts) >= 4: base_out["CHAKSUN4"] = parts[3]
                if len(parts) >= 5: base_out["CHAKSUN5"] = parts[4]

    # 2. Extract horse rows from .RaceTable01
    results = []
    table = soup.select_one(".RaceTable01")
    if not table:
        return results
        
    rows = table.select("tr.HorseList")
    for row in rows:
        # Base dict copy for each horse
        row_dict = base_out.copy()
        
        # Additional fields purely for the horse row
        row_dict["CHULNO"] = None
        row_dict["HRNAME"] = None
        row_dict["HRNO"] = None
        row_dict["PRD"] = None
        row_dict["SEX"] = None
        row_dict["AGE"] = None
        row_dict["HR_LAST_AMT"] = None
        row_dict["WGBUDAM"] = None
        row_dict["RATING"] = None
        row_dict["JKNAME"] = None
        row_dict["JKNO"] = None
        row_dict["TRNAME"] = None
        row_dict["TRNO"] = None
        row_dict["OWNAME"] = None
        row_dict["OWNO"] = None
        row_dict["PRIZECOND"] = None
        row_dict["CHAKSUNT"] = None
        row_dict["CHAKSUNY"] = None
        row_dict["CHAKSUN_6M"] = None
        row_dict["ORD1CNTT"] = None
        row_dict["ORD2CNTT"] = None
        row_dict["ORD3CNTT"] = None
        row_dict["RCCNTT"] = None
        row_dict["ORD1CNTY"] = None
        row_dict["ORD2CNTY"] = None
        row_dict["ORD3CNTY"] = None
        row_dict["RCCNTY"] = None
        
        tds = row.find_all("td", recursive=False)
        if len(tds) < 8:
            continue
            
        # CHULNO
        chulno_text = tds[1].get_text(strip=True)
        if chulno_text.isdigit():
            row_dict["CHULNO"] = int(chulno_text)
            
        # HRNAME & HRNO
        hr_tag = tds[3].select_one("a")
        if hr_tag:
            row_dict["HRNAME"] = hr_tag.get_text(strip=True)
            m = re.search(r"horse/(20\d+)", hr_tag.get('href', ''))
            if not m: m = re.search(r"horse/(\d+)", hr_tag.get('href', ''))
            if m: row_dict["HRNO"] = m.group(1)
            else: row_dict["HRNAME"] = tds[3].get_text(strip=True)
            
        # SEX & AGE
        barei_text = tds[4].get_text(strip=True)
        if barei_text:
            m_sex = re.search(r"([牝牡セセン]+)", barei_text)
            m_age = re.search(r"(\d+)", barei_text)
            if m_sex:
                sex_val = m_sex.group(1)
                row_dict["SEX"] = "セン" if sex_val == "セ" else sex_val # Normalize
            if m_age: row_dict["AGE"] = int(m_age.group(1))
            
        # WGBUDAM
        wgbudam_text = tds[5].get_text(strip=True)
        if wgbudam_text:
            try:
                row_dict["WGBUDAM"] = float(wgbudam_text)
            except ValueError:
                pass
                
        # JKNAME & JKNO
        jk_td = tds[6] if len(tds) > 6 else None
        if jk_td:
            jk_a = jk_td.select_one("a")
            if jk_a:
                row_dict["JKNAME"] = jk_a.get_text(strip=True)
                m = re.search(r"/jockey/.*?(\d{4,5})", jk_a.get('href', ''))
                if m: row_dict["JKNO"] = m.group(1)
            else:
                row_dict["JKNAME"] = jk_td.get_text(strip=True)
                
        # TRNAME & TRNO
        tr_td = tds[7] if len(tds) > 7 else None
        if tr_td:
            tr_a = tr_td.select_one("a")
            if tr_a:
                row_dict["TRNAME"] = tr_a.get_text(strip=True)
                m = re.search(r"/trainer/.*?(\d{4,5})", tr_a.get('href', ''))
                if m: row_dict["TRNO"] = m.group(1)
            else:
                row_dict["TRNAME"] = tr_td.get_text(strip=True)
                
        results.append(row_dict)

    return results
