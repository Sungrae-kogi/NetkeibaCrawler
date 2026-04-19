import csv
import time
import random
import sys
from pathlib import Path
import requests
from bs4 import BeautifulSoup
from parser import parse_api_entry_sheet_2

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)"
}

def main():
    base_url_template = "https://race.netkeiba.com/race/shutuba.html?race_id={}"
    if len(sys.argv) > 1:
        start_race_id = sys.argv[1]
    else:
        start_race_id = "202609020701"
    
    # 만약 url이 통째로 들어왔다면 맨 뒤 race_id 파라미터만 추출
    if "race_id=" in start_race_id:
        import re
        m = re.search(r"race_id=(\d+)", start_race_id)
        if m:
            start_race_id = m.group(1)
            
    prefix = start_race_id[:-2]
    all_entries = []

    # 자체 메타데이터 저장용 폴더
    base_dir = Path(__file__).resolve().parent
    data_dir = base_dir / "data"
    data_dir.mkdir(parents=True, exist_ok=True)

    print(f"========== api_entry_sheet_2 수집 시작 (Base ID: {prefix}) ==========")
    for i in range(1, 13):
        race_id = f"{prefix}{i:02d}"
        url = base_url_template.format(race_id)
        
        print(f"[{i:02d}/12] 요청 중: {url}")
        try:
            r = requests.get(url, headers=HEADERS, timeout=15)
            r.encoding = "EUC-JP"
            r.raise_for_status()
            
            soup = BeautifulSoup(r.text, "lxml")
            if not soup.select_one(".RaceList_Item02"):
                print(f"  -> 경기가 존재하지 않습니다. 당일 순회 종료.")
                break
                
            entries = parse_api_entry_sheet_2(soup, url)
            all_entries.extend(entries)
            race_name = entries[0]['RCNAME'] if entries else '알수없는경기'
            print(f"  -> 수집 성공: {race_name} (출전마 {len(entries)}마리 추가됨)")

        except requests.exceptions.HTTPError as e:
            if r.status_code == 404:
                print(f"  -> 경기 {i}를 찾을 수 없습니다(404). 순회 종료.")
                break
            else:
                print(f"  -> HTTP 에러: {e}")
        except Exception as e:
            print(f"  -> 에러 발생: {e}")
            
        time.sleep(random.uniform(0.7, 1.5))

    # 3. CSV 덤프 저장 및 PK 추출
    if all_entries:
        # User DDL에 맞춘 Column 순서
        fieldnames = [
            "MEET", "RCDATE", "RCDAY", "RCNO", "CHULNO", "HRNAME", "HRNO", "PRD",
            "SEX", "AGE", "HR_LAST_AMT", "WGBUDAM", "RATING", "JKNAME", "JKNO",
            "TRNAME", "TRNO", "OWNAME", "OWNO", "ILSU", "RCDIST", "DUSU",
            "RANK", "PRIZECOND", "AGECOND", "STTIME", "BUDAM", "RCNAME", "CHAKSUN1",
            "CHAKSUN2", "CHAKSUN3", "CHAKSUN4", "CHAKSUN5", "CHAKSUNT", "CHAKSUNY",
            "CHAKSUN_6M", "ORD1CNTT", "ORD2CNTT", "ORD3CNTT", "RCCNTT", "ORD1CNTY",
            "ORD2CNTY", "ORD3CNTY", "RCCNTY"
        ]
        # 덤프 파일명 결정
        first_row = all_entries[0]
        date_str = str(first_row.get("RCDATE") or "unknown").replace("-", "").replace("/", "").strip()
        meet_str = str(first_row.get("MEET") or "unknown").strip()
        out_csv = data_dir / f"api_entry_sheet_2_{meet_str}_{date_str}.csv"
        
        with open(out_csv, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(all_entries)
        print(f"\n[완료] 결과 저장: {out_csv} (총 {len(all_entries)}줄의 출전마 행이 생성됨)")

        # PK 추출 및 분배 저장 로직 추가
        pks = {"HRNO": set(), "JKNO": set(), "TRNO": set()}
        for row in all_entries:
            if row.get("HRNO"): pks["HRNO"].add(row["HRNO"])
            if row.get("JKNO"): pks["JKNO"].add(row["JKNO"])
            if row.get("TRNO"): pks["TRNO"].add(row["TRNO"])
            
        # PycharmProjects 루트 디렉토리
        root_dir = base_dir.parent.parent 
        targets = {
            "HRNO": root_dir / "HRNOCrawler" / "nodata",
            "JKNO": root_dir / "JKNOCrawler" / "nodata",
            "TRNO": root_dir / "TRNOCrwaler" / "nodata"
        }
        
        for key, folder_path in targets.items():
            folder_path.mkdir(parents=True, exist_ok=True)
            out_file = folder_path / f"{key}_{meet_str}_{date_str}_list.csv"
            with open(out_file, "w", newline="", encoding="utf-8-sig") as f:
                writer = csv.writer(f)
                writer.writerow([key])
                for item in sorted(pks[key]):
                    writer.writerow([item])
                    
        print(f"[완료] PK 분배 저장: HRNO({len(pks['HRNO'])}), JKNO({len(pks['JKNO'])}), TRNO({len(pks['TRNO'])}) [nodata 폴더]")
    else:
        print("[경고] 수집된 데이터가 없습니다.")

if __name__ == "__main__":
    main()
