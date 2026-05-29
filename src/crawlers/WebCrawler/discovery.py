import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import re
import logging

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("Discovery")

TARGET_VENUES = {
    "東京": "05",
    "中山": "06",
    "阪神": "09",
    "京都": "08"
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Referer": "https://www.netkeiba.com/"
}

BASE_URL = "https://race.netkeiba.com"

def get_upcoming_dates():
    """오늘로부터 가장 가까운 토/일/월 날짜 3개를 반환합니다."""
    today = datetime.now()
    dates = []
    # 오늘 포함 앞으로 7일간 조사
    for i in range(7):
        target = today + timedelta(days=i)
        dates.append(target.strftime("%Y%m%d"))
    return dates

def discover_races(date_str):
    """특정 날짜의 페이지에서 대상 개최지(4대 경기장)의 1R race_id를 추출합니다."""
    # 비동기로 로드되는 경기 목록 HTML을 직접 요청
    url = f"{BASE_URL}/top/race_list_sub.html?kaisai_date={date_str}"
    try:
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        response.encoding = "EUC-JP"
        soup = BeautifulSoup(response.content, "html.parser")
        
        found_targets = []
        # 경기장 이름표(.RaceList_DataTitle)를 기준으로 탐색
        titles = soup.select(".RaceList_DataTitle")
        
        if not titles:
            # 상단 날짜 탭에서 넘어가야 할 수 있으므로 텍스트로도 확인
            if any(name in response.text for name in TARGET_VENUES.keys()):
                logger.debug(f"{date_str} 페이지에서 대상 지역 키워드는 발견되었으나 타이틀 요소가 없습니다.")
            return []

        for title in titles:
            # 1. 타이틀 텍스트에서 경기장 이름 추출
            header_text = title.get_text(strip=True)
            venue_name = ""
            for name in TARGET_VENUES.keys():
                if name in header_text:
                    venue_name = name
                    break
            
            if not venue_name:
                continue

            # 2. 해당 타이틀 이후에 나타나는 모든 요소들 중 첫 번째 주소 탐색
            # (다음 타이틀이 나오기 전까지만 찾아야 함)
            r1_url = ""
            r1_id = ""
            
            # 타이틀 이후의 모든 형제/하위 요소를 순회
            for sibling in title.find_all_next():
                # 만약 다른 경기장 타이틀을 만났다면 이 경기장 영역은 끝난 것임
                if "RaceList_DataTitle" in sibling.get('class', []):
                    break
                
                # 경주 링크(race_id) 발견 시
                if sibling.name == "a" and "race_id=" in sibling.get('href', ''):
                    href = sibling.get('href', '')
                    match = re.search(r"race_id=(\d{10,12})", href)
                    if match:
                        full_id = match.group(1)
                        r1_id = full_id[:-2] + "01"
                        r1_url = f"{BASE_URL}/race/shutuba.html?race_id={r1_id}&rf=race_list"
                        break
            
            if r1_url:
                found_targets.append({
                    "date": date_str,
                    "venue": venue_name,
                    "race_id": r1_id,
                    "url": r1_url
                })
                logger.info(f"🎯 [발견] {date_str} {venue_name} -> {r1_url}")
        
        return found_targets
    except Exception as e:
        logger.error(f"{date_str} 탐색 중 오류: {e}")
        return []

def get_all_target_races():
    """주말 및 연기된 경기를 동적으로 탐색합니다."""
    dates = get_upcoming_dates()
    all_targets = []
    for d in dates:
        results = discover_races(d)
        if results:
            all_targets.extend(results)
    return all_targets

if __name__ == "__main__":
    targets = get_all_target_races()
    print("\n--- [Discovery 결과] ---")
    for t in targets:
        print(f"{t['date']} | {t['venue']} | {t['url']}")
