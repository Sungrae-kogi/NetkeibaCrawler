import os
import csv
import asyncio
import argparse
import logging
from pathlib import Path
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

BASE_DIR = Path(__file__).resolve().parent
LOG_DIR = BASE_DIR.parent / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
logger = logging.getLogger("ImageDownloader")
if not logger.handlers:
    logger.setLevel(logging.INFO)
    ch = logging.StreamHandler()
    ch.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
    logger.addHandler(ch)

async def download_horse_images(hrno: str, max_images: int = 15):
    hrno = hrno.strip()
    if len(hrno) != 10:
        logger.error(f"올바르지 않은 HRNO 형식입니다 (10자리 숫자 필요): {hrno}")
        return

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        state_path = BASE_DIR.parent / "storage_state.json"
        context_args = {}
        if state_path.exists():
            context_args["storage_state"] = str(state_path)
            
        context = await browser.new_context(**context_args)
        page = await context.new_page()

        try:
            # 1. 말 이름 추출
            horse_url = f"https://db.netkeiba.com/horse/{hrno}"
            logger.info(f"[{hrno}] 접속: {horse_url}")
            await page.goto(horse_url, timeout=30000)
            
            try:
                name_el = await page.wait_for_selector("div.horse_title h1", timeout=10000)
                horse_name = (await name_el.inner_text()).strip()
            except PlaywrightTimeoutError:
                logger.error(f"[{hrno}] 말 이름을 찾을 수 없습니다.")
                return

            logger.info(f"[{hrno}] 말 이름 추출: {horse_name}")
            
            save_dir = Path(f"Z:\\JMaFeel\\경주마 영상\\경주마\\{horse_name}")
            
            # 2. 사진 목록 접속
            photo_list_url = f"https://db.netkeiba.com/photo/list.html?id={hrno}"
            logger.info(f"[{hrno}] 사진 목록 접속: {photo_list_url}")
            await page.goto(photo_list_url, timeout=30000)
            
            # 썸네일 링크에서 원본 이미지 URL(show_photo.php) 직접 추출
            links = await page.evaluate('''() => {
                const anchors = Array.from(document.querySelectorAll('a'));
                return anchors
                    .map(a => a.href)
                    .filter(href => href.includes('show_photo.php'));
            }''')

            unique_links = list(dict.fromkeys(links))
            
            if not unique_links:
                logger.warning(f"[{hrno}] 등록된 사진이 없거나 구조가 변경되었습니다.")
                return
                
            target_links = unique_links[:max_images]
            logger.info(f"[{hrno}] 총 {len(unique_links)}개 사진 중 {len(target_links)}개 다운로드 시도 (최대 {max_images}장 제한)")
            
            # 디렉터리 생성
            save_dir.mkdir(parents=True, exist_ok=True)
            logger.info(f"[{hrno}] 저장 경로: {save_dir}")

            # 3. 원본 이미지 직접 다운로드
            for idx, link in enumerate(target_links, start=1):
                img_filename = f"{horse_name}_{idx:02d}.jpg"
                img_path = save_dir / img_filename
                
                if img_path.exists():
                    logger.info(f"[{hrno}] 이미 존재함 (건너뜀): {img_filename}")
                    continue
                    
                import random
                delay = random.uniform(1.5, 3.0)
                await asyncio.sleep(delay)  # 차단 방지를 위한 랜덤 딜레이
                
                logger.info(f"[{hrno}] 사진 {idx} 다운로드 중...")
                try:
                    # 상세 페이지를 거치지 않고 바로 이미지 URL로 GET 요청
                    max_retries = 3
                    downloaded = False
                    for attempt in range(1, max_retries + 1):
                        response = await page.request.get(link, timeout=30000)
                        if response.ok:
                            image_data = await response.body()
                            with open(img_path, "wb") as f:
                                f.write(image_data)
                            logger.info(f"[{hrno}] 다운로드 완료: {img_filename}")
                            downloaded = True
                            break
                        else:
                            logger.warning(f"[{hrno}] 사진 {idx} 다운로드 실패 (상태코드: {response.status}). {attempt}/{max_retries} 재시도 중...")
                            await asyncio.sleep(2)
                            
                    if not downloaded:
                        logger.error(f"[{hrno}] 사진 {idx} 최종 다운로드 실패.")
                        
                except Exception as inner_e:
                    logger.error(f"[{hrno}] 사진 {idx} 처리 중 오류: {inner_e}")
                    continue

        except Exception as e:
            logger.error(f"[{hrno}] 작업 중 오류 발생: {e}")
        finally:
            await browser.close()
            
def run_downloader(hrno_list, max_images=30):
    for hrno in hrno_list:
        asyncio.run(download_horse_images(hrno, max_images))

def load_hrno_list_from_csv(csv_path: Path, col_name: str = "HRNO") -> list[str]:
    hrnos = []
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            v = (row.get(col_name) or "").strip()
            if v:
                hrnos.append(v)
    seen = set()
    uniq = []
    for x in hrnos:
        if x not in seen:
            seen.add(x)
            uniq.append(x)
    return uniq
        
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Netkeiba 경주마 사진 다운로더")
    parser.add_argument("--csv", help="HRNO 목록이 포함된 CSV 파일 경로")
    parser.add_argument("hrnos", nargs="*", help="직접 입력할 HRNO 목록 (CSV 사용 시 무시됨)")
    args = parser.parse_args()

    hrno_list = []
    if args.csv:
        csv_path = Path(args.csv)
        if csv_path.exists():
            hrno_list = load_hrno_list_from_csv(csv_path)
            logger.info(f"CSV에서 {len(hrno_list)}개의 HRNO를 성공적으로 로드했습니다: {csv_path.name}")
        else:
            logger.error(f"CSV 파일을 찾을 수 없습니다: {args.csv}")
            import sys
            sys.exit(1)
    elif args.hrnos:
        hrno_list = args.hrnos
    else:
        # 기본 테스트용
        hrno_list = ["2020105154"]

    if not hrno_list:
        logger.error("다운로드할 HRNO가 없습니다.")
    else:
        run_downloader(hrno_list, max_images=30)
