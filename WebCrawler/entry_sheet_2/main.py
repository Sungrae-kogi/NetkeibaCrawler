import csv
import time
import random
import sys
from pathlib import Path
import requests
from bs4 import BeautifulSoup
from parser import parse_api_entry_sheet_2

# 자동 인증 모듈 추가
sys.path.append(str(Path(__file__).resolve().parent.parent.parent))
from netkeiba_auth import get_netkeiba_cookies

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

    # 자동 인증 모듈을 통해 쿠키 획득
    print("🔑 프리미엄 세션 자동 확인 중...")
    try:
        cookies = get_netkeiba_cookies()
    except Exception as e:
        print(f"⚠️ 세션 확보 실패 (일반 모드로 진행): {e}")
        cookies = {}

    print(f"========== api_entry_sheet_2 수집 시작 (Base ID: {prefix}) ==========")
    
    any_failed = False
    # 전 경기(1~12경주) 수집
    for i in range(1, 13):
        race_id = f"{prefix}{i:02d}"
        url = base_url_template.format(race_id)
        
        print(f"[{i:02d}/12] 요청 중: {url}")
        try:
            r = requests.get(url, headers=HEADERS, cookies=cookies, timeout=15)
            r.encoding = "EUC-JP"
            r.raise_for_status()
            
            soup = BeautifulSoup(r.text, "lxml")
            if not soup.select_one(".RaceList_Item02"):
                print(f"  -> 경기가 존재하지 않습니다. 당일 순회 종료.")
                break
                
            entries = parse_api_entry_sheet_2(soup, url)
            if not entries:
                print(f"  -> 경고: {race_id}에서 데이터를 파싱하지 못했습니다.")
                any_failed = True
                continue

            all_entries.extend(entries)
            race_name = entries[0]['RCNAME']
            print(f"  -> 수집 성공: {race_name} (출전마 {len(entries)}마리 추가됨)")

        except Exception as e:
            print(f"  -> 에러 발생 ({race_id}): {e}")
            any_failed = True
            
        time.sleep(random.uniform(0.7, 1.5))

    # 3. CSV 덤프 저장 및 PK 추출
    if all_entries:
        # User DDL에 맞춘 Column 순서
        fieldnames = [
            "MEET", "RCDATE", "RCDAY", "RCNO", "WAKU", "CHULNO", "HRNAME", "HRNO",
            "SEX", "AGE", "WGBUDAM", "JKNAME", "JKNO",
            "TRNAME", "TRNO", "TRACK_TYPE", "DIRECTION", "RCDIST", "DUSU",
            "RANK", "AGECOND", "STTIME", "RCNAME", "CHAKSUN1",
            "CHAKSUN2", "CHAKSUN3", "CHAKSUN4", "CHAKSUN5"
        ]
        
        first_row = all_entries[0]
        final_date = str(first_row.get("RCDATE") or "unknown").replace("-", "").replace("/", "").strip()
        final_meet = str(first_row.get("MEET") or "unknown").strip()
        final_out_csv = data_dir / f"api_entry_sheet_2_{final_meet}_{final_date}.csv"

        # 항상 덮어쓰기 (사용자 요청)
        with open(final_out_csv, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(all_entries)
        print(f"\n[완료] 결과 저장(Overwrite): {final_out_csv} (총 {len(all_entries)}줄)")

        # PK 추출 및 분배 저장 로직
        pks = {"HRNO": set(), "JKNO": set(), "TRNO": set()}
        for row in all_entries:
            if row.get("HRNO"): pks["HRNO"].add(row["HRNO"])
            if row.get("JKNO"): pks["JKNO"].add(row["JKNO"])
            if row.get("TRNO"): pks["TRNO"].add(row["TRNO"])
            
        root_dir = base_dir.parent.parent 
        targets = {
            "HRNO": root_dir / "HRNOCrawler" / "nodata",
            "JKNO": root_dir / "JKNOCrawler" / "nodata",
            "TRNO": root_dir / "TRNOCrwaler" / "nodata"
        }
        
        for key, folder_path in targets.items():
            folder_path.mkdir(parents=True, exist_ok=True)
            out_file = folder_path / f"{key}_{final_meet}_{final_date}_list.csv"
            with open(out_file, "w", newline="", encoding="utf-8-sig") as f:
                writer = csv.writer(f)
                writer.writerow([key])
                for item in sorted(pks[key]):
                    writer.writerow([item])
                    
        print(f"[완료] PK 분배 저장: HRNO({len(pks['HRNO'])}), JKNO({len(pks['JKNO'])}), TRNO({len(pks['TRNO'])})")
    else:
        print("[경고] 수집된 데이터가 없습니다.")

    if any_failed:
        sys.exit(2)

if __name__ == "__main__":
    main()
