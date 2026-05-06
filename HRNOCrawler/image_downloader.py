import csv
import asyncio
import argparse
import logging
import random
import re
from datetime import datetime
from pathlib import Path
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

BASE_DIR = Path(__file__).resolve().parent

# 파일 로그 설정 추가
LOG_DIR = BASE_DIR.parent / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
date_str = datetime.now().strftime("%Y%m%d")
LOG_FILE = LOG_DIR / f"{date_str}_ImageDownloader.log"

logger = logging.getLogger("ImageDownloader")
if not logger.handlers:
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
    
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    
    fh = logging.FileHandler(LOG_FILE, encoding='utf-8')
    fh.setFormatter(formatter)
    logger.addHandler(fh)

def log_failed_hrno(hrno, reason, file_suffix=""):
    suffix = file_suffix if file_suffix else date_str
    failed_csv = LOG_DIR / f"failed_hrno_{suffix}.csv"
    file_exists = failed_csv.exists()
    try:
        with open(failed_csv, "a", encoding="utf-8-sig", newline="") as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow(["HRNO", "Reason", "Timestamp"])
            writer.writerow([hrno, reason, datetime.now().strftime("%Y-%m-%d %H:%M:%S")])
    except Exception as e:
        logger.error(f"실패 기록 저장 중 오류: {e}")

async def fetch_and_download_photos(page, hrno, save_dir, prefix, max_images, horse_name_for_log):
    """
    특정 HRNO의 사진 목록 페이지에서 사진을 수집하고 저장합니다.
    """
    photo_list_url = f"https://db.netkeiba.com/photo/list.html?id={hrno}"
    logger.info(f"[{hrno}] 사진 목록 접속: {photo_list_url}")
    
    try:
        await page.goto(photo_list_url, timeout=60000)
    except Exception as e:
        logger.error(f"[{hrno}] 사진 목록 접속 실패: {e}")
        return 0

    # 1. '더보기' 버튼 클릭 루프 (최대 max_images 확보를 위해)
    try:
        for _ in range(5):
            current_links_count = await page.evaluate('''() => {
                return Array.from(document.querySelectorAll('a'))
                    .filter(a => a.href.includes('show_photo.php')).length;
            }''')
            
            if current_links_count >= max_images:
                break
                
            more_btn = await page.query_selector("#MoreColumn01, a.MoreBtn_01")
            if more_btn and await more_btn.is_visible():
                logger.info(f"[{hrno}] '더보기' 버튼 클릭 중... (현재 로드된 사진: {current_links_count}장)")
                await more_btn.click()
                await asyncio.sleep(2)
            else:
                break
    except Exception as more_e:
        logger.warning(f"[{hrno}] '더보기' 버튼 처리 중 오류 (무시하고 진행): {more_e}")

    # 2. 썸네일 링크에서 원본 이미지 URL(show_photo.php) 추출
    links = await page.evaluate('''() => {
        return Array.from(document.querySelectorAll('a'))
            .map(a => a.href)
            .filter(href => href.includes('show_photo.php'));
    }''')

    unique_links = list(dict.fromkeys(links))
    if not unique_links:
        return 0
        
    target_links = unique_links[:max_images]
    logger.info(f"[{hrno}] 총 {len(unique_links)}개 사진 중 {len(target_links)}개 다운로드 시도 ({prefix} 수집)")
    
    # 디렉터리 생성
    save_dir.mkdir(parents=True, exist_ok=True)

    # 3. 이미지 다운로드
    download_count = 0
    for idx, link in enumerate(target_links, start=1):
        img_filename = f"{prefix}_{idx:02d}.jpg"
        img_path = save_dir / img_filename
        
        if img_path.exists():
            download_count += 1
            continue
            
        await asyncio.sleep(random.uniform(1.0, 2.0))
        
        try:
            response = await page.request.get(link, timeout=60000)
            if response.ok:
                with open(img_path, "wb") as f:
                    f.write(await response.body())
                download_count += 1
            else:
                logger.warning(f"[{hrno}] {img_filename} 다운로드 실패 (상태코드: {response.status})")
        except Exception as e:
            logger.error(f"[{hrno}] {img_filename} 처리 중 오류: {e}")
            
    return download_count

async def get_parent_info(page):
    """
    현재 말 프로필 페이지에서 부마(Sire)와 모마(Dam)의 ID 정보를 추출합니다.
    (외국마의 영문 포함 ID 및 /ped/ 경로 대응)
    """
    parents = {"sire": None, "dam": None}
    try:
        # 혈통표(table.blood_table)가 로드될 때까지 대기
        await page.wait_for_selector("table.blood_table", timeout=30000)
        
        # 부마: 첫 번째 행 첫 번째 칸의 첫 번째 a 태그
        sire_el = await page.query_selector("table.blood_table tr:nth-child(1) td:nth-child(1) a")
        if sire_el:
            href = await sire_el.get_attribute("href")
            # /horse/12345/ 또는 /horse/ped/12345/ 또는 영문 포함 ID 대응
            m = re.search(r'/horse/(?:ped/)?(\w+)/', href)
            if m: 
                parents["sire"] = m.group(1)
                logger.info(f"🧬 부마 ID 추출 성공: {parents['sire']}")
            
        # 모마: 세 번째 행 첫 번째 칸의 첫 번째 a 태그
        dam_el = await page.query_selector("table.blood_table tr:nth-child(3) td:nth-child(1) a")
        if dam_el:
            href = await dam_el.get_attribute("href")
            m = re.search(r'/horse/(?:ped/)?(\w+)/', href)
            if m: 
                parents["dam"] = m.group(1)
                logger.info(f"🧬 모마 ID 추출 성공: {parents['dam']}")
                
        if not parents["sire"] or not parents["dam"]:
            logger.warning("⚠️ 일부 부모 정보를 추출하지 못했습니다. (구조 확인 필요)")
            
    except Exception as e:
        logger.error(f"❌ 부모 정보 추출 중 오류: {e}")
    return parents



async def process_single_horse(page, hrno: str, max_images: int = 30):
    hrno = hrno.strip()
    if len(hrno) != 10:
        logger.error(f"올바르지 않은 HRNO 형식입니다: {hrno}")
        return False, "올바르지 않은 HRNO 형식"

    try:
        # 1. 메인 프로필 페이지 접속
        horse_url = f"https://db.netkeiba.com/horse/{hrno}"
        logger.info(f"[{hrno}] 프로필 접속: {horse_url}")
        await page.goto(horse_url, timeout=60000)
        
        # 말 이름 추출
        try:
            name_el = await page.wait_for_selector("div.horse_title h1", timeout=30000)
            horse_name = (await name_el.inner_text()).strip()
        except Exception as e:
            logger.error(f"[{hrno}] 말 이름을 찾을 수 없습니다.", exc_info=True)
            return False, "말 이름 추출 실패 (타임아웃 또는 구조 변경)"

        save_dir = Path(f"Z:\\JMaFeel\\경주마 영상\\경주마\\{horse_name}")
        silk_path = save_dir / "마주복색.jpg"

        # [최적화] 이미 데이터가 충분하다면 건너뛰기
        if save_dir.exists() and silk_path.exists():
            # 1. 본인 사진이 있는지 확인
            other_jpgs = [f for f in save_dir.iterdir() if f.suffix.lower() == '.jpg' and f.name != "마주복색.jpg"]
            # 2. 부모 폴더(백업 데이터)가 이미 존재하는지 확인
            has_parent_folders = (save_dir / "부마").exists() or (save_dir / "모마").exists()

            if other_jpgs or has_parent_folders:
                logger.info(f"[{hrno}] 이미 본인 또는 부모 사진 수집이 완료된 말입니다 ({horse_name}). 작업을 건너뜁니다.")
                return True, "이미 수집 완료됨"

        save_dir.mkdir(parents=True, exist_ok=True)


        # 2. 마주복색 수집
        try:
            silk_img_el = await page.query_selector("table.db_prof_table tr:has(th:has-text('馬主')) td img")
            if silk_img_el:
                silk_url = await silk_img_el.get_attribute("src")
                if silk_url:
                    if silk_url.startswith("//"): silk_url = "https:" + silk_url
                    elif not silk_url.startswith("http"): silk_url = "https://db.netkeiba.com" + silk_url
                    
                    if not silk_path.exists():
                        res = await page.request.get(silk_url, timeout=60000)
                        if res.ok:
                            with open(silk_path, "wb") as f: f.write(await res.body())
                            logger.info(f"[{hrno}] 마주복색 저장 완료")
                        else:
                            return False, f"마주복색 다운로드 실패 (상태코드: {res.status})"
            else:
                return False, "마주복색(馬主) 이미지를 찾을 수 없습니다"
        except Exception as e:
            logger.warning(f"[{hrno}] 마주복색 수집 실패: {e}", exc_info=True)
            return False, f"마주복색 수집 중 예외 발생"

        # 3. 부모 정보 미리 추출 (백업용)
        parent_info = await get_parent_info(page)

        # 4. 본인 사진 수집 시도
        downloaded = await fetch_and_download_photos(page, hrno, save_dir, horse_name, max_images, horse_name)

        # 5. [백업 로직] 본인 사진이 0장일 경우 부모 사진 수집
        if downloaded == 0:
            logger.info(f"[{hrno}] 본인 사진이 없습니다. 부모 사진 백업 수집을 시작합니다.")
            backup_success = False
            
            # 부마 사진 수집
            if parent_info["sire"]:
                sire_dir = save_dir / "부마"
                sire_dl = await fetch_and_download_photos(page, parent_info["sire"], sire_dir, "부마", 16, horse_name)
                if sire_dl > 0: backup_success = True
            
            # 모마 사진 수집
            if parent_info["dam"]:
                dam_dir = save_dir / "모마"
                dam_dl = await fetch_and_download_photos(page, parent_info["dam"], dam_dir, "모마", 16, horse_name)
                if dam_dl > 0: backup_success = True
                
            if not backup_success:
                return False, "본인 사진 0장 및 부모 사진 백업 실패"
        
        logger.info(f"[{hrno}] {horse_name} 수집 작업 완료")
        return True, "성공"

    except Exception as e:
        logger.error(f"[{hrno}] 작업 중 오류: {e}", exc_info=True)
        return False, f"전체 작업 중 예외 발생: {e}"

async def run_downloader_async(hrno_list, max_images=30, file_suffix=""):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        state_path = BASE_DIR.parent / "storage_state.json"
        context_args = {
            "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        }
        if state_path.exists():
            context_args["storage_state"] = str(state_path)
            
        context = await browser.new_context(**context_args)
        page = await context.new_page()

        try:
            for idx, hrno in enumerate(hrno_list, start=1):
                logger.info(f"--- [{idx}/{len(hrno_list)}] {hrno} 처리 시작 ---")
                
                success = False
                reason = ""
                for attempt in range(1, 4):
                    if attempt > 1:
                        logger.info(f"[{hrno}] 수집 재시도 ({attempt}/3)...")
                        
                    res, reason = await process_single_horse(page, hrno, max_images)
                    if res:
                        success = True
                        break
                    else:
                        logger.warning(f"[{hrno}] 수집 실패 ({attempt}/3 사유: {reason})")
                        if attempt < 3:
                            delay = attempt * 10
                            logger.info(f"[{hrno}] {delay}초 대기 후 재시도합니다...")
                            await asyncio.sleep(delay)
                
                if not success:
                    logger.error(f"[{hrno}] 최종 수집 실패. failed_hrno.csv 에 기록합니다.")
                    log_failed_hrno(hrno, reason, file_suffix)
                
                # 말과 말 사이에 딜레이를 주어 봇 탐지 방지 및 서버 안정성 확보
                if idx < len(hrno_list):
                    delay = random.uniform(3.0, 7.0)
                    logger.info(f"다음 말 수집 전 {delay:.1f}초 대기 중...\n")
                    await asyncio.sleep(delay)
        finally:
            await browser.close()

def run_downloader(hrno_list, max_images=30, file_suffix=""):
    if not hrno_list:
        return
    asyncio.run(run_downloader_async(hrno_list, max_images, file_suffix))

def load_hrno_list_from_csv(csv_path: Path) -> list[str]:
    hrnos = []
    try:
        with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                v = (row.get("HRNO") or "").strip()
                if v: hrnos.append(v)
    except Exception as e:
        logger.error(f"CSV 로드 오류: {e}")
    return list(dict.fromkeys(hrnos))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Netkeiba 경주마 사진 다운로더 (백업 기능 포함)")
    parser.add_argument("--csv", help="HRNO 목록 CSV 경로")
    parser.add_argument("hrnos", nargs="*", help="직접 입력 HRNO")
    args = parser.parse_args()

    hrno_list = []
    file_suffix = ""
    
    if args.csv:
        csv_path = Path(args.csv)
        hrno_list = load_hrno_list_from_csv(csv_path)
        name = csv_path.stem
        if name.startswith("HRNO_"): name = name[5:]
        if name.endswith("_list"): name = name[:-5]
        file_suffix = name
    elif args.hrnos:
        hrno_list = args.hrnos

    if hrno_list:
        run_downloader(hrno_list, file_suffix=file_suffix)
    else:
        logger.error("대상 HRNO가 없습니다.")
