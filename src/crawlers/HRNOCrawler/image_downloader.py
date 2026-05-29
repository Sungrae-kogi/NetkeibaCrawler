import os
import csv
import shutil
import asyncio
import argparse
import logging
import random
import subprocess
import time
from pathlib import Path
from datetime import datetime
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

BASE_DIR = Path(__file__).resolve().parent
LOG_DIR = BASE_DIR.parent.parent.parent / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
logger = logging.getLogger("ImageDownloader")
if not logger.handlers:
    logger.setLevel(logging.INFO)
    
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
    
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    
    log_file = LOG_DIR / f"image_downloader_{datetime.now().strftime('%Y%m%d')}.log"
    fh = logging.FileHandler(log_file, encoding='utf-8')
    fh.setFormatter(formatter)
    logger.addHandler(fh)

def reconnect_vpn():
    """NordVPN CLI를 사용하여 VPN 연결을 재시작합니다."""
    nordvpn_path = r"C:\Program Files\NordVPN\nordvpn.exe"
    if not Path(nordvpn_path).exists():
        logger.error(f"NordVPN 실행 파일을 찾을 수 없습니다: {nordvpn_path}")
        return
        
    logger.info("🌐 [NordVPN] VPN 연결 해제 중...")
    try:
        subprocess.run([nordvpn_path, "-d"], shell=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        time.sleep(3)
        logger.info("🌐 [NordVPN] VPN 재연결 중 (Japan 서버)...")
        subprocess.run([nordvpn_path, "-c", "-g", "Japan"], shell=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        logger.info("🌐 [NordVPN] 재연결 완료! 네트워크 안정화를 위해 10초 대기합니다.")
        time.sleep(10)
    except Exception as e:
        logger.error(f"🌐 [NordVPN] 제어 중 예외 발생: {e}")

def ensure_vpn_connected():
    """초기 실행 시 NordVPN 연결 상태를 보장합니다. 꺼져있으면 켜고, 켜져있으면 유지합니다."""
    nordvpn_path = r"C:\Program Files\NordVPN\nordvpn.exe"
    if not Path(nordvpn_path).exists():
        logger.error(f"NordVPN 실행 파일을 찾을 수 없습니다: {nordvpn_path}")
        return
        
    logger.info("🌐 [NordVPN] 초기 연결 상태를 확인하고 Japan 서버로 연결을 보장합니다...")
    try:
        # -c -g Japan 명령어는 꺼져있으면 켜고, 켜져있으면 Japan 서버로 연결을 유지/변경합니다.
        subprocess.run([nordvpn_path, "-c", "-g", "Japan"], shell=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        time.sleep(5) # 연결 안정화를 위한 대기
    except Exception as e:
        logger.error(f"🌐 [NordVPN] 초기 연결 중 예외 발생: {e}")

def log_failed_hrno(hrno, horse_name, reason):
    failed_csv = LOG_DIR / "failed_hrno.csv"
    file_exists = failed_csv.exists()
    try:
        with open(failed_csv, "a", encoding="utf-8-sig", newline="") as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow(["HRNO", "HorseName", "Reason", "Timestamp"])
            writer.writerow([hrno, horse_name, reason, datetime.now().strftime("%Y-%m-%d %H:%M:%S")])
    except Exception as e:
        logger.error(f"실패 기록 저장 중 오류: {e}")

async def _download_photos_from_list(page, target_hrno, horse_name, save_dir, max_images, is_target=False):
    photo_list_url = f"https://db.netkeiba.com/photo/list.html?id={target_hrno}"
    logger.info(f"[{target_hrno}] 사진 목록 접속: {photo_list_url}")
    
    max_list_retries = 3
    page_loaded = False
    
    for attempt in range(1, max_list_retries + 1):
        try:
            photo_list_res = await page.goto(photo_list_url, timeout=30000)
            if photo_list_res:
                if photo_list_res.status in [403, 503, 502, 504]:
                    logger.warning(f"⚠️ [{target_hrno}] 사진 목록 접속 차단(상태코드 {photo_list_res.status}). VPN 재연결 시도 ({attempt}/{max_list_retries})")
                    reconnect_vpn()
                    continue
            
            # [보완] 페이지 DOM 로딩 보장 및 동적 렌더링(Network Idle) 대기 추가
            await page.wait_for_load_state("load")
            try:
                await page.wait_for_load_state("networkidle", timeout=5000)
            except Exception:
                pass
                
            # [보완] 사진 태그(show_photo.php)가 나타날 때까지 실시간 감시 대기
            try:
                await page.wait_for_selector("a[href*='show_photo.php']", timeout=5000)
            except Exception:
                pass
                
            page_loaded = True
            break
        except PlaywrightTimeoutError:
            logger.warning(f"⚠️ [{target_hrno}] 사진 목록 접속 타임아웃 발생. VPN 재연결 시도 ({attempt}/{max_list_retries})")
            reconnect_vpn()
        except Exception as e:
            logger.warning(f"⚠️ [{target_hrno}] 사진 목록 접속 오류: {e} ({attempt}/{max_list_retries})")
            await asyncio.sleep(random.uniform(2.0, 4.0))

    if not page_loaded:
        logger.error(f"❌ [{target_hrno}] 사진 목록 접속 최종 실패.")
        return -1
    
    links = await page.evaluate('''() => {
        const anchors = Array.from(document.querySelectorAll('a'));
        return anchors
            .map(a => a.href)
            .filter(href => href.includes('show_photo.php'));
    }''')

    unique_links = list(dict.fromkeys(links))
    
    if not unique_links:
        logger.info(f"[{target_hrno}] 등록된 사진이 없습니다 (True Zero).")
        return 0
        
    target_links = unique_links[:max_images]
    logger.info(f"[{target_hrno}] 총 {len(unique_links)}개 사진 중 {len(target_links)}개 다운로드 시도")
    
    downloaded_count = 0
    for idx, link in enumerate(target_links, start=1):
        img_filename = f"{horse_name}_{idx:02d}.jpg"
        img_path = save_dir / img_filename
        
        if img_path.exists():
            logger.info(f"[{target_hrno}] 이미 존재함 (건너뜀): {img_filename}")
            downloaded_count += 1
            continue
            
        delay = random.uniform(1.5, 3.0)
        await asyncio.sleep(delay)
        
        logger.info(f"[{target_hrno}] 사진 {idx} 다운로드 중...")
        try:
            max_img_retries = 3
            downloaded = False
            for attempt in range(1, max_img_retries + 1):
                response = await page.request.get(link, timeout=30000)
                if response.ok:
                    image_data = await response.body()
                    with open(img_path, "wb") as f:
                        f.write(image_data)
                    logger.info(f"[{target_hrno}] 다운로드 완료: {img_filename}")
                    downloaded = True
                    downloaded_count += 1
                    break
                elif response.status == 404:
                    logger.warning(f"[{target_hrno}] 사진 {idx} 서버에 없음 (404). 스킵.")
                    break
                elif response.status in [403, 502, 503]:
                    logger.warning(f"[{target_hrno}] 사진 {idx} 다운로드 차단(상태 {response.status}). VPN 교체...")
                    reconnect_vpn()
                else:
                    logger.warning(f"[{target_hrno}] 사진 {idx} 다운로드 실패 ({response.status}). {attempt}/{max_img_retries} 재시도...")
                    await asyncio.sleep(2)
                    
            if not downloaded:
                logger.error(f"[{target_hrno}] 사진 {idx} 최종 다운로드 실패.")
                
        except Exception as inner_e:
            logger.error(f"[{target_hrno}] 사진 {idx} 처리 중 오류: {inner_e}")
            continue
            
    if downloaded_count == 0 and len(target_links) > 0:
        logger.error(f"❌ [{target_hrno}] 사진이 존재하지만 단 한 장도 받지 못해 실패 처리합니다.")
        return -1
        
    return downloaded_count

async def download_horse_images(hrno: str, max_images: int = 15) -> bool:
    hrno = hrno.strip()
    horse_name = "알수없음"
    save_dir = None
    success = False
    
    if len(hrno) != 10:
        logger.error(f"올바르지 않은 HRNO 형식입니다 (10자리 숫자 필요): {hrno}")
        log_failed_hrno(hrno, horse_name, "올바르지 않은 HRNO 형식")
        return False

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        state_path = BASE_DIR.parent.parent.parent / "storage_state.json"
        
        # 헤더 설정으로 봇 탐지 우회
        context_args = {
            "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
            "extra_http_headers": {
                "Accept-Language": "ja-JP,ja;q=0.9,ko-KR;q=0.8,ko;q=0.7,en-US;q=0.6,en;q=0.5"
            }
        }
        
        if state_path.exists():
            context_args["storage_state"] = str(state_path)
            
        context = await browser.new_context(**context_args)
        page = await context.new_page()
        
        try:
            # 1. 말 이름 추출
            horse_url = f"https://db.netkeiba.com/horse/{hrno}"
            logger.info(f"[{hrno}] 접속: {horse_url}")
            
            max_page_retries = 5
            page_loaded = False
            is_404 = False
            
            for attempt in range(1, max_page_retries + 1):
                try:
                    response = await page.goto(horse_url, timeout=30000)
                    if response:
                        if response.status == 404:
                            logger.error(f"[{hrno}] HTTP 404 - 해당 말이 존재하지 않음.")
                            is_404 = True
                            break
                        if response.status in [403, 502, 503, 504]:
                            logger.warning(f"[{hrno}] 차단 의심(상태코드 {response.status}). VPN 재연결 시도 ({attempt}/{max_page_retries})")
                            reconnect_vpn()
                            continue
                            
                    name_el = await page.wait_for_selector("div.horse_title h1", timeout=15000)
                    horse_name = (await name_el.inner_text()).strip()
                    page_loaded = True
                    break
                except PlaywrightTimeoutError as e:
                    logger.warning(f"[{hrno}] 타임아웃 발생(차단 의심). VPN 재연결 시도 ({attempt}/{max_page_retries})")
                    reconnect_vpn()
                except Exception as e:
                    logger.warning(f"[{hrno}] 접속 오류: {e} ({attempt}/{max_page_retries})")
                    await asyncio.sleep(random.uniform(2.0, 4.0))

            if is_404:
                log_failed_hrno(hrno, horse_name, "HTTP 404 - 말 정보 자체가 없음")
                return False

            if not page_loaded:
                logger.error(f"[{hrno}] VPN 교체 등 재시도 후에도 말 이름을 찾을 수 없습니다 (최종 실패).")
                log_failed_hrno(hrno, horse_name, "말 이름 추출 최종 실패 (VPN 재연결 초과)")
                return False

            logger.info(f"[{hrno}] 말 이름 추출: {horse_name}")
            
            save_dir = Path(f"Z:\\JMaFeel\\경주마 영상\\경주마\\{horse_name}")
            save_dir.mkdir(parents=True, exist_ok=True)
            
            # 마주복색 다운로드
            silk_path = save_dir / "마주복색.jpg"
            silk_img_el = await page.query_selector("table.db_prof_table tr:has(th:has-text('馬主')) td img")
            if not silk_img_el:
                logger.error(f"❌ [{hrno}] 마주복색 태그가 페이지에 존재하지 않아 수집 실패 처리합니다.")
                log_failed_hrno(hrno, horse_name, "[마주복색] 이미지 구조 없음")
                return False

            silk_url = await silk_img_el.get_attribute("src")
            if not silk_url:
                logger.error(f"❌ [{hrno}] 마주복색 URL을 찾을 수 없습니다.")
                log_failed_hrno(hrno, horse_name, "[마주복색] URL 누락")
                return False

            if silk_url.startswith("//"):
                silk_url = "https:" + silk_url
            elif not silk_url.startswith("http"):
                silk_url = "https://db.netkeiba.com" + silk_url

            silk_downloaded = False
            if silk_path.exists():
                logger.info(f"[{hrno}] 마주복색 이미지가 이미 존재합니다.")
                silk_downloaded = True
            else:
                max_silk_retries = 3
                for attempt in range(1, max_silk_retries + 1):
                    try:
                        res = await page.request.get(silk_url, timeout=30000)
                        if res.ok:
                            image_data = await res.body()
                            with open(silk_path, "wb") as f:
                                f.write(image_data)
                            logger.info(f"[{hrno}] 마주복색 저장 완료")
                            silk_downloaded = True
                            break
                        elif res.status == 404:
                            logger.error(f"❌ [{hrno}] 마주복색 사진이 서버에 없음 (404)")
                            break
                        else:
                            logger.warning(f"⚠️ [{hrno}] 마주복색 다운로드 실패 (상태코드: {res.status}). {attempt}/{max_silk_retries-1} 재시도 대기...")
                    except Exception as e:
                        logger.warning(f"⚠️ [{hrno}] 마주복색 다운로드 시도 중 오류: {e}. {attempt}/{max_silk_retries-1} 재시도 대기...")
                    
                    if attempt < max_silk_retries:
                        time.sleep(3)

            if not silk_downloaded:
                logger.error(f"❌ [{hrno}] 마주복색 수집 최종 실패.")
                log_failed_hrno(hrno, horse_name, "[마주복색] 다운로드 최종 실패")
                return False
            
            # 2. 본인 사진 다운로드 시도 (테스트를 위해 빠른 진행 1개 수집)
            download_result = await _download_photos_from_list(page, hrno, horse_name, save_dir, max_images=10, is_target=True)
            
            if download_result == -1:
                logger.error(f"❌ [{hrno}] 본인 사진 목록 접속 또는 이미지 다운로드 최종 실패.")
                return False
                
            elif download_result == 0:
                logger.warning(f"[{hrno}] 본인 등록된 사진이 없음 (True Zero). 부마/모마 사진 수집을 시도합니다.")
                
                # 프로필 페이지로 복귀하여 혈통표에서 부마/모마 링크 추출
                await asyncio.sleep(random.uniform(2.5, 4.5))
                await page.goto(horse_url, timeout=30000)
                
                sire_href = await page.evaluate('''() => {
                    const a = document.querySelector("table.blood_table td.b_ml a");
                    return a ? a.href : null;
                }''')
                
                dam_href = await page.evaluate('''() => {
                    const a = document.querySelector("table.blood_table td.b_fml a");
                    return a ? a.href : null;
                }''')
                
                # 부마 사진 수집
                sire_success = False
                if sire_href:
                    sire_hrno = sire_href.rstrip('/').split('/')[-1]
                    logger.info(f"[{hrno}] 부마 수집 시작 (ID: {sire_hrno})")
                    
                    try:
                        sire_page_loaded = False
                        for attempt in range(1, 4):
                            try:
                                await asyncio.sleep(random.uniform(2.5, 4.5))
                                await page.goto(sire_href, timeout=30000)
                                
                                # '写真' 탭 클릭하여 사진 목록으로 이동
                                photo_link = await page.query_selector("a:text('写真')")
                                if photo_link:
                                    await photo_link.click()
                                    await page.wait_for_load_state("load", timeout=20000)
                                else:
                                    await page.goto(f"https://db.netkeiba.com/photo/list.html?id={sire_hrno}", timeout=30000)
                                
                                sire_name_el = await page.wait_for_selector("div.horse_title h1", timeout=15000)
                                sire_name = (await sire_name_el.inner_text()).strip()
                                sire_name = sire_name.replace("의投稿사진", "").replace("の投稿写真", "").replace("사진", "").replace("写真", "").strip()
                                
                                sire_page_loaded = True
                                break
                            except Exception as e:
                                logger.warning(f"[{hrno}] 부마 페이지 접속 시도 {attempt}/3 실패: {e}")
                                reconnect_vpn()
                                
                        if sire_page_loaded:
                            sire_dir = save_dir / "부마"
                            sire_dir.mkdir(parents=True, exist_ok=True)
                            logger.info(f"[{hrno}] 부마({sire_name}) 사진 수집 중 (최대 10장)...")
                            await asyncio.sleep(random.uniform(2.5, 4.5))
                            sire_res = await _download_photos_from_list(page, sire_hrno, sire_name, sire_dir, max_images=10, is_target=False)
                            if sire_res > 0:
                                sire_success = True
                            else:
                                if sire_res != -1:
                                    sire_success = True # 수집 시도는 정상(True Zero)으로 마감하되 폴더만 삭제
                                try:
                                    if sire_dir.exists():
                                        sire_dir.rmdir()
                                        logger.info(f"🧹 [{hrno}] 다운로드된 부마 사진이 없어 빈 '{sire_dir.name}' 폴더를 정리했습니다.")
                                except Exception as del_e:
                                    logger.error(f"🧹 [{hrno}] 빈 부마 폴더 정리 실패: {del_e}")
                    except Exception as e:
                        logger.warning(f"[{hrno}] 부마 정보 파악/수집 중 최종 오류: {e}")
                else:
                    logger.warning(f"[{hrno}] 부마 링크를 찾을 수 없습니다.")
                
                # 본체 페이지로 복귀하여 모마 링크 찾기 대기
                await asyncio.sleep(random.uniform(2.0, 4.0))
                await page.goto(horse_url, timeout=30000)
                    
                # 모마 사진 수집
                dam_success = False
                if dam_href:
                    dam_hrno = dam_href.rstrip('/').split('/')[-1]
                    logger.info(f"[{hrno}] 모마 수집 시작 (ID: {dam_hrno})")
                    
                    try:
                        dam_page_loaded = False
                        for attempt in range(1, 4):
                            try:
                                await asyncio.sleep(random.uniform(2.5, 4.5))
                                await page.goto(dam_href, timeout=30000)
                                
                                # '写真' 탭 클릭하여 사진 목록으로 이동
                                photo_link = await page.query_selector("a:text('写真')")
                                if photo_link:
                                    await photo_link.click()
                                    await page.wait_for_load_state("load", timeout=20000)
                                else:
                                    await page.goto(f"https://db.netkeiba.com/photo/list.html?id={dam_hrno}", timeout=30000)
                                
                                dam_name_el = await page.wait_for_selector("div.horse_title h1", timeout=15000)
                                dam_name = (await dam_name_el.inner_text()).strip()
                                dam_name = dam_name.replace("의投稿사진", "").replace("の投稿写真", "").replace("사진", "").replace("写真", "").strip()
                                
                                dam_page_loaded = True
                                break
                            except Exception as e:
                                logger.warning(f"[{hrno}] 모마 페이지 접속 시도 {attempt}/3 실패: {e}")
                                reconnect_vpn()
                                
                        if dam_page_loaded:
                            dam_dir = save_dir / "모마"
                            dam_dir.mkdir(parents=True, exist_ok=True)
                            logger.info(f"[{hrno}] 모마({dam_name}) 사진 수집 중 (최대 10장)...")
                            await asyncio.sleep(random.uniform(2.5, 4.5))
                            dam_res = await _download_photos_from_list(page, dam_hrno, dam_name, dam_dir, max_images=10, is_target=False)
                            if dam_res > 0:
                                dam_success = True
                            else:
                                if dam_res != -1:
                                    dam_success = True # 수집 시도는 정상(True Zero)으로 마감하되 폴더만 삭제
                                try:
                                    if dam_dir.exists():
                                        dam_dir.rmdir()
                                        logger.info(f"🧹 [{hrno}] 다운로드된 모마 사진이 없어 빈 '{dam_dir.name}' 폴더를 정리했습니다.")
                                except Exception as del_e:
                                    logger.error(f"🧹 [{hrno}] 빈 모마 폴더 정리 실패: {del_e}")
                    except Exception as e:
                        logger.warning(f"[{hrno}] 모마 정보 파악/수집 중 최종 오류: {e}")
                else:
                    logger.warning(f"[{hrno}] 모마 링크를 찾을 수 없습니다.")

                if (sire_href and not sire_success) or (dam_href and not dam_success):
                    logger.error(f"❌ [{hrno}] 부마/모마 사진 수집 중 네트워크 오류 또는 차단으로 최종 실패했습니다.")
                    log_failed_hrno(hrno, horse_name, "부마/모마 수집 중 접속 실패")
                    return False

            success = True

        except Exception as e:
            logger.error(f"[{hrno}] 작업 중 오류 발생: {e}")
            log_failed_hrno(hrno, horse_name, f"전체 작업 중 예외 발생: {e}")
            return False
        finally:
            await browser.close()
            if not success and save_dir and save_dir.exists():
                try:
                    shutil.rmtree(save_dir)
                    logger.warning(f"🧹 [{hrno}] 수집 최종 실패로 인해 임시 생성된 '{horse_name}' 폴더를 롤백(삭제)합니다.")
                except Exception as del_e:
                    logger.error(f"🧹 [{hrno}] 폴더 삭제 실패: {del_e}")
            
        return success
            
def run_downloader(hrno_list, max_images=30):
    ensure_vpn_connected()
    total = len(hrno_list)
    success_count = 0
    fail_count = 0
    skip_count = 0
    for item in hrno_list:
        if isinstance(item, dict):
            hrno = item.get("HRNO")
            hrname = item.get("HRNAME", "")
        else:
            hrno = item
            hrname = ""
            
        # 디렉토리 존재 여부 사전 검사
        if hrname:
            check_dir = Path(f"Z:\\JMaFeel\\경주마 영상\\경주마\\{hrname}")
            if check_dir.exists() and check_dir.is_dir():
                logger.info(f"⏭️ [{hrno}] '{hrname}' 디렉토리가 이미 존재합니다. 사이트 방문 없이 건너뜁니다.")
                skip_count += 1
                continue
                
        success = asyncio.run(download_horse_images(hrno, max_images))
        if success:
            success_count += 1
        else:
            fail_count += 1
            
    logger.info(f"========== 전체 다운로드 완료 ==========")
    logger.info(f"총 대상 말 수: {total} 마리")
    logger.info(f"✅ 수집 성공: {success_count} 마리")
    logger.info(f"⏭️ 건너뜀(이미 존재): {skip_count} 마리")
    logger.info(f"❌ 수집 실패: {fail_count} 마리")
    logger.info(f"========================================")

def load_hrno_list_from_csv(csv_path: Path, col_name: str = "HRNO") -> list[dict]:
    items = []
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            hrno = (row.get(col_name) or "").strip()
            hrname = (row.get("HRNAME") or "").strip()
            if hrno:
                items.append({"HRNO": hrno, "HRNAME": hrname})
    seen = set()
    uniq = []
    for x in items:
        if x["HRNO"] not in seen:
            seen.add(x["HRNO"])
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
        hrno_list = ["2020105154"]

    if not hrno_list:
        logger.error("다운로드할 HRNO가 없습니다.")
    else:
        run_downloader(hrno_list, max_images=30)
