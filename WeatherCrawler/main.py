import requests
from bs4 import BeautifulSoup
import csv
import logging
from pathlib import Path
from datetime import datetime
import re
import json

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("WeatherCrawler")

BASE_URL = "https://race.netkeiba.com/race/track.html"
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"

# 경기장별 ID (netkeiba 내부 ID)
VENUE_IDS = {
    "東京": "05",
    "中山": "06",
    "阪神": "09",
    "京都": "08"
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Referer": "https://www.netkeiba.com/"
}

def fetch_weather_and_track(date_str):
    """지정한 날짜의 모든 경기장 날씨 및 바바 상태를 수집합니다."""
    url = f"{BASE_URL}?kaisai_date={date_str}"
    try:
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        response.encoding = "EUC-JP"  # 인코딩 명시
        soup = BeautifulSoup(response.text, "html.parser")
        
        # 1. 자바스크립트 내의 날씨 예보 데이터(암호문) 추출
        # 예: let weather_forecasts = {"05":["02", "|", "03"]};
        forecast_map = {}
        script_tags = soup.find_all("script")
        for script in script_tags:
            if script.string and "weather_forecasts" in script.string:
                match = re.search(r"weather_forecasts\s*=\s*({.*?});", script.string)
                if match:
                    try:
                        forecast_map = json.loads(match.group(1))
                        logger.info(f"🔮 동적 예보 데이터 확보 성공: {len(forecast_map)}개 지역")
                    except:
                        pass
                    break

        # 날씨 번역 사전
        WEATHER_CODE = {
            "01": "晴", "02": "曇", "03": "雨", "04": "小雨", "05": "雪", "06": "小雪"
        }
        SEP_CODE = {
            "|": "時々", "/": "のち"
        }

        results = []
        for venue_name, v_id in VENUE_IDS.items():
            # 각 지역별 컨테이너 탐색
            container = soup.select_one(f".TrackTabArea00.Jyo_{v_id}")
            if not container:
                logger.warning(f"{date_str} {venue_name} 경기장 정보를 찾을 수 없습니다.")
                continue
            
            # --- 날씨(WEATHER) 추출 로직 ---
            weather = "unknown"
            # 우선순위 1: 자바스크립트 예보 데이터 해독
            if v_id in forecast_map:
                codes = forecast_map[v_id]
                decoded = ""
                for c in codes:
                    if c in WEATHER_CODE: decoded += WEATHER_CODE[c]
                    elif c in SEP_CODE: decoded += SEP_CODE[c]
                    else: decoded += c
                weather = decoded
                logger.info(f"🎯 {venue_name} 예보 해독 결과: {weather}")
            else:
                # 우선순위 2: 기존 HTML 텍스트 방식 (확정된 경기 등)
                weather_elem = container.select_one(".CourseData .Weather")
                if weather_elem:
                    weather_text = weather_elem.get_text(strip=True)
                    weather = re.sub(r"^天候.*?：", "", weather_text).split("|")[0].strip()

            # 만약 여전히 "-" 이거나 비어있다면 해독 실패로 간주
            if weather in ["-", "", "unknown"] and v_id not in forecast_map:
                weather = "unknown"

            # --- 바장 상태(TURF/DIRT) 추출 로직 ---
            # 잔디 상태 추출 (芝)
            turf_elem = container.select_one(".TrackTurf01 .CourseData span:nth-child(2)")
            if turf_elem:
                turf_text = turf_elem.get_text(strip=True)
                turf = re.sub(r"^馬場.*?：", "", turf_text).split("|")[0].strip()
            else:
                turf = "none"
            
            # 더트 상태 추출 (ダート)
            dirt_elem = container.select_one(".TrackDirt01 .CourseData span:nth-child(2)")
            if dirt_elem:
                dirt_text = dirt_elem.get_text(strip=True)
                dirt = re.sub(r"^馬場.*?：", "", dirt_text).split("|")[0].strip()
            else:
                dirt = "none"
            
            results.append({
                "DATE": date_str,
                "VENUE": venue_name,
                "WEATHER": weather,
                "TURF": turf,
                "DIRT": dirt,
                "FETCH_TIME": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            })
            logger.info(f"✅ {date_str} {venue_name} -> 날씨: {weather} | 잔디: {turf} | 더트: {dirt}")
            
        return results
    except Exception as e:
        logger.error(f"{date_str} 날씨 수집 중 오류 발생: {e}")
        return []

def save_to_csv(data, date_str):
    """수집된 날씨 정보를 루트 data 폴더에 CSV로 저장합니다."""
    if not data:
        return
    
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    out_path = DATA_DIR / f"Weather_Track_{date_str}.csv"
    
    file_exists = out_path.exists()
    keys = data[0].keys()
    
    with open(out_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(data)
    
    logger.info(f"💾 데이터 저장 완료: {out_path}")

def run_weather_crawl(date_str):
    """외부에서 호출 가능한 진입점"""
    logger.info(f"🌦️ {date_str} 날씨 및 바바 정보 수집 시작...")
    data = fetch_weather_and_track(date_str)
    if data:
        save_to_csv(data, date_str)
        return True
    return False

if __name__ == "__main__":
    # 테스트용: 오늘 날짜로 실행
    today = datetime.now().strftime("%Y%m%d")
    run_weather_crawl(today)
