import csv
import json
import time
import hashlib
import requests
import re
import msvcrt
from datetime import datetime
from bs4 import BeautifulSoup
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
CACHE_FILE = DATA_DIR / "seen_cache.json"
CSV_FILE = DATA_DIR / "extracted_info.csv"
CANCEL_CSV_FILE = DATA_DIR / "cancel_extracted_info.csv"

URL = "https://race.netkeiba.com/top/information.html?rf=sidemenu"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)"
}

def load_cache():
    """기존 파싱된 정보의 해시값 목록을 불러옵니다."""
    if CACHE_FILE.exists():
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            return set(json.load(f))
    return set()

def save_cache(cache_set):
    """현재까지 파싱된 정보의 해시값 목록을 캐시 파일에 저장합니다."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(list(cache_set), f, ensure_ascii=False, indent=2)

def generate_hash(text):
    """문자열에 대한 MD5 해시값을 생성합니다."""
    return hashlib.md5(text.encode('utf-8')).hexdigest()

def fetch_and_parse():
    now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{now_str}] 넷케이바 Information 갱신 내역 확인 중...")
    try:
        r = requests.get(URL, headers=HEADERS, timeout=15)
        # 넷케이바 인코딩 보정
        r.encoding = "EUC-JP"
        r.raise_for_status()
    except Exception as e:
        print(f"[오류] 페이지 구조를 가져오는데 실패했습니다: {e}")
        return

    soup = BeautifulSoup(r.text, "lxml")
    
    # 해당 사이트는 클래스명에 오타(Infomation)가 있음.
    # 혹시 나중에 고쳐질 수도 있으니 두 가지 경우를 모두 대비
    items = soup.select("div.Race_Information ul.Infomation > li")
    if not items:
        items = soup.select("div.Race_Information ul.Information > li")
    
    seen_cache = load_cache()
    new_records = []
    
    for li in items:
        # 카테고리 (출주 취소, 기수 변경 등)
        cat_elem = li.select_one("dt.Link_Title span")
        # 대상 경기/장소 (예: 토요 나카야마 6R)
        place_elem = li.select_one("dt.Link_Title div")
        # 내용 및 업로드 시간 
        sub_txt = li.select_one("dd.Sub_Txt")
        
        category = cat_elem.text.strip() if cat_elem else ""
        place = place_elem.text.strip() if place_elem else ""
        details = sub_txt.text.strip() if sub_txt else ""
        
        # 고유 식별자 생성
        combined_text = f"{category}|{place}|{details}"
        item_hash = generate_hash(combined_text)
        
        # 처음 보는 정보(해시)라면 새로 등록
        if item_hash not in seen_cache:
            seen_cache.add(item_hash)
            new_records.append({
                "CRAWL_TIME": now_str,
                "CATEGORY": category,
                "PLACE": place,
                "DETAILS": details,
                "HASH_ID": item_hash
            })
            
    if new_records:
        print(f"  -> 🎉 새로운 정보 {len(new_records)}건이 발견되어 저장합니다!")
        save_csv(new_records)
        
        # [데이터 분리 추출] 출주 취소인 건만 따로 파싱하여 저장
        cancel_records = []
        for rec in new_records:
            if rec["CATEGORY"] == "出走取消":
                cancel_records.append(parse_cancel_record(rec))
                
        if cancel_records:
            save_cancel_csv(cancel_records)
            print(f"     (출주 취소 데이터 {len(cancel_records)}건 파싱 및 분리 저장 완료)")
            
        save_cache(seen_cache)
        for rec in reversed(new_records):
            print(f"     [{rec['CATEGORY']}] {rec['PLACE']} - {rec['DETAILS']}")
    else:
        print("  -> 새로운 업데이트가 없습니다.")

def save_csv(records):
    """새로 감지된 정보들을 CSV에 누적(Append)하여 저장합니다."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    file_exists = CSV_FILE.exists()
    
    fieldnames = ["CRAWL_TIME", "CATEGORY", "PLACE", "DETAILS", "HASH_ID"]
    
    with open(CSV_FILE, "a", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        # 파일이 없었다면 헤더 먼저 작성
        if not file_exists:
            writer.writeheader()
        
        # 위에서부터 파싱하면 최신 데이터가 먼저 들어옴.
        # 기록은 시간 순서(과거->최신)로 남기기 위해 뒤집어서 저장
        for row in reversed(records):
            writer.writerow(row)

def parse_cancel_record(record):
    """出走取消 타입의 데이터를 정규식으로 분해하여 새 구조로 반환합니다."""
    out = {
        "CRAWL_TIME": record["CRAWL_TIME"],
        "CATEGORY": record["CATEGORY"],
        "RCDAY": "",
        "MEET": "",
        "RCNO": "",
        "CHULNO": "",
        "HRNAME": "",
        "RCDATE": ""
    }
    
    place = record.get("PLACE", "")
    details = record.get("DETAILS", "")
    
    # PLACE 파싱: 예) 土曜阪神6R -> RCDAY=土, MEET=阪神, RCNO=6
    m_place = re.match(r"^(.)曜(.+?)(\d+)R", place)
    if m_place:
        out["RCDAY"] = m_place.group(1)
        out["MEET"] = m_place.group(2)
        out["RCNO"] = m_place.group(3)
        
    # DETAILS 파싱: 예) 2番 エイコーンドリーム (4/18 12:43) 
    # -> CHULNO=2, HRNAME=エイコーンドリーム, 날짜추출 후 RCDATE=20260418 생성
    fixed_details = details.replace('\xa0', ' ')
    m_details = re.search(r"(\d+)番\s*([^\(]+?)\s*\(\s*(\d+)/(\d+)", fixed_details)
    if m_details:
        out["CHULNO"] = m_details.group(1)
        out["HRNAME"] = m_details.group(2).strip()
        month = int(m_details.group(3))
        day = int(m_details.group(4))
        year = datetime.now().year
        out["RCDATE"] = f"{year}{month:02d}{day:02d}"
        
    return out

def save_cancel_csv(records):
    """파싱된 出走取消 데이터를 별도의 CSV(cancel_extracted_info.csv)에 누적 저장합니다."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    file_exists = CANCEL_CSV_FILE.exists()
    
    fieldnames = ["CRAWL_TIME", "CATEGORY", "RCDAY", "MEET", "RCNO", "CHULNO", "HRNAME", "RCDATE"]
    
    with open(CANCEL_CSV_FILE, "a", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        
        for row in reversed(records):
            writer.writerow(row)

def sleep_with_cancel(seconds):
    """지정된 초만큼 대기하며 'q' 입력 시 중단합니다."""
    for _ in range(seconds):
        if msvcrt.kbhit():
            key = msvcrt.getch().decode('utf-8').lower()
            if key == 'q':
                return True
        time.sleep(1)
    return False

def main():
    print("========== 🐎 Netkeiba Information 모니터링 봇 시작 ==========")
    print("종료하시려면 터미널 창을 닫거나 키보드에서 [Ctrl + C] 를 누르세요.")
    print("==============================================================\n")
    
    while True:
        fetch_and_parse()
        print("1시간 뒤에 다시 확인합니다... (대기 중, 중단하고 메뉴로 돌아가려면 'q' 입력)\n")
        if sleep_with_cancel(3600):
            print("\n[안내] 사용자에 의해 모니터링 대기가 중단되었습니다.")
            break

if __name__ == "__main__":
    main()
