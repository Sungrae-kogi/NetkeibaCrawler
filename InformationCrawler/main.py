import csv
import json
import time
import hashlib
import requests
from datetime import datetime
from bs4 import BeautifulSoup
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
CACHE_FILE = DATA_DIR / "seen_cache.json"
CSV_FILE = DATA_DIR / "extracted_info.csv"

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

def main():
    print("========== 🐎 Netkeiba Information 모니터링 봇 시작 ==========")
    print("종료하시려면 터미널 창을 닫거나 키보드에서 [Ctrl + C] 를 누르세요.")
    print("==============================================================\n")
    
    while True:
        fetch_and_parse()
        print("30분 뒤에 다시 확인합니다... (대기 중)\n")
        # 1800초 = 30분
        time.sleep(1800)

if __name__ == "__main__":
    main()
