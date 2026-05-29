import os
import sys
import asyncio
import logging
from pathlib import Path
from playwright.async_api import async_playwright

# 루트 디렉토리 참조 추가
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(BASE_DIR))

# 상세 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("SingleImageTest")

async def test_single_horse_image(hrno: str, headless: bool = True):
    logger.info(f"🚀 [{hrno}] 단일 말 이미지 수집 테스트 시작 (headless={headless})")
    
    # 임시 테스트 저장 경로 설정 (Z 드라이브 대신 프로젝트 내 local 디렉토리 사용)
    test_save_dir = BASE_DIR / "tests" / "test_downloads" / hrno
    test_save_dir.mkdir(parents=True, exist_ok=True)
    logger.info(f"💾 테스트 저장 경로: {test_save_dir}")

    async with async_playwright() as p:
        # 브라우저 실행
        browser = await p.chromium.launch(headless=headless)
        
        # storage_state.json 확인 및 로드 (쿠키 재사용을 위해)
        state_path = BASE_DIR / "storage_state.json"
        context_args = {
            "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
            "extra_http_headers": {
                "Accept-Language": "ja-JP,ja;q=0.9,ko-KR;q=0.8,ko;q=0.7,en-US;q=0.6,en;q=0.5"
            }
        }
        
        if state_path.exists():
            context_args["storage_state"] = str(state_path)
            logger.info("🔑 storage_state.json을 쿠키 세션으로 로드합니다.")
        else:
            logger.warning("⚠️ storage_state.json 세션 파일을 찾을 수 없습니다. 비로그인 일반 모드로 테스트합니다.")
            
        context = await browser.new_context(**context_args)
        page = await context.new_page()

        try:
            # 1단계: 말 프로필 상세 페이지 접속 및 말 이름 추출
            horse_url = f"https://db.netkeiba.com/horse/{hrno}"
            logger.info(f"🌐 1단계 - 프로필 페이지 접속: {horse_url}")
            
            response = await page.goto(horse_url, timeout=30000)
            if response:
                logger.info(f"📡 응답 상태 코드: {response.status}")
                if response.status == 404:
                    logger.error("❌ HTTP 404: 해당 말이 존재하지 않습니다.")
                    return
            
            # 페이지 로딩 대기 후 스크린샷 저장
            await page.wait_for_load_state("load")
            screenshot_path = test_save_dir / "profile_page.png"
            await page.screenshot(path=str(screenshot_path))
            logger.info(f"📸 프로필 페이지 스크린샷 저장 완료: {screenshot_path.name}")

            # 이름 추출
            try:
                name_el = await page.wait_for_selector("div.horse_title h1", timeout=10000)
                horse_name = (await name_el.inner_text()).strip()
                logger.info(f"🐎 말 이름 파싱 성공: {horse_name}")
            except Exception as e:
                logger.error(f"❌ 말 이름 파싱 실패: {e}")
                # HTML 저장
                html_path = test_save_dir / "profile_page_error.html"
                with open(html_path, "w", encoding="utf-8") as f:
                    f.write(await page.content())
                logger.info(f"📄 디버깅용 HTML 저장 완료: {html_path.name}")
                return

            # 2단계: 사진 목록 페이지 접속
            photo_list_url = f"https://db.netkeiba.com/photo/list.html?id={hrno}"
            logger.info(f"🌐 2단계 - 사진 목록 페이지 접속: {photo_list_url}")
            
            list_response = await page.goto(photo_list_url, timeout=30000)
            if list_response:
                logger.info(f"📡 사진 목록 응답 코드: {list_response.status}")
            
            await page.wait_for_load_state("load")
            
            # 사진 목록 페이지 스크린샷 및 HTML 덤프 저장 (가장 중요)
            list_screenshot = test_save_dir / "photo_list_page.png"
            await page.screenshot(path=str(list_screenshot))
            logger.info(f"📸 사진 목록 페이지 스크린샷 저장 완료: {list_screenshot.name}")
            
            list_html = test_save_dir / "photo_list_page.html"
            with open(list_html, "w", encoding="utf-8") as f:
                f.write(await page.content())
            logger.info(f"📄 사진 목록 페이지 HTML 덤프 저장 완료: {list_html.name}")

            # 3단계: 페이지 안의 모든 a 태그 수집 및 show_photo.php 패턴 추출
            logger.info("🔍 a 태그 분석 시작...")
            anchors_info = await page.evaluate('''() => {
                const anchors = Array.from(document.querySelectorAll('a'));
                return anchors.map(a => ({
                    text: a.innerText.trim(),
                    href: a.href
                }));
            }''')
            
            logger.info(f"📑 페이지 내 발견된 총 a 태그 수: {len(anchors_info)}")
            
            # 디버깅용 전체 a 태그 출력 (일부만)
            logger.info("=== 일부 a 태그 목록 (처음 20개) ===")
            for idx, a in enumerate(anchors_info[:20]):
                logger.info(f"  [{idx+1}] Text: {a['text']} | Href: {a['href']}")
            
            # show_photo.php 포함 링크 필터링
            links = [a['href'] for a in anchors_info if 'show_photo.php' in a['href']]
            unique_links = list(dict.fromkeys(links))
            
            logger.info(f"🎯 'show_photo.php'를 포함하는 유니크 링크 개수: {len(unique_links)}")
            if unique_links:
                for idx, link in enumerate(unique_links[:5], 1):
                    logger.info(f"  발견된 사진 링크 {idx}: {link}")
            else:
                logger.warning("⚠️ 'show_photo.php' 패턴을 포함하는 링크가 단 하나도 발견되지 않았습니다.")
                
                # 혹시 다른 패턴의 이미지 뷰어 링크가 있는지 추가 탐색
                photo_viewer_links = [a['href'] for a in anchors_info if 'photo' in a['href'].lower() and 'list' not in a['href'].lower()]
                logger.info(f"💡 대안 패턴 ('photo' 포함하되 'list' 제외) 링크 수: {len(photo_viewer_links)}")
                for idx, link in enumerate(photo_viewer_links[:5], 1):
                    logger.info(f"  대안 사진 링크 {idx}: {link}")
                    
                # <img> 태그 직접 분석
                imgs_info = await page.evaluate('''() => {
                    return Array.from(document.querySelectorAll('img')).map(img => img.src);
                }''')
                logger.info(f"🖼️ 페이지 내 발견된 img 태그 수: {len(imgs_info)}")
                for idx, img_src in enumerate(imgs_info[:5], 1):
                    logger.info(f"  이미지 {idx} src: {img_src}")

            # 4단계: 실제 이미지 파일 다운로드 시도
            if unique_links:
                logger.info("⚡ 4단계 - 첫 번째 이미지 다운로드 테스트...")
                test_link = unique_links[0]
                img_path = test_save_dir / f"{horse_name}_test_01.jpg"
                
                try:
                    img_response = await page.request.get(test_link, timeout=15000)
                    if img_response.ok:
                        image_data = await img_response.body()
                        with open(img_path, "wb") as f:
                            f.write(image_data)
                        logger.info(f"✅ 다운로드 성공: {img_path.name} ({len(image_data)} bytes)")
                    else:
                        logger.error(f"❌ 다운로드 응답 실패: 상태 코드 {img_response.status}")
                except Exception as e:
                    logger.error(f"❌ 이미지 다운로드 요청 중 예외 발생: {e}")
            else:
                logger.error("❌ 다운로드 테스트를 진행할 이미지 링크가 없습니다.")

        except Exception as e:
            logger.error(f"🔥 테스트 실행 중 오류 발생: {e}")
        finally:
            await browser.close()
            logger.info("🏁 테스트 종료 및 브라우저 세션 해제 완료")

if __name__ == "__main__":
    # 테스트 대상 말 ID: 2023106883 (그리온ヴール)
    target_hrno = "2023106883"
    
    # headless=False로 설정하면 브라우저 동작 화면을 직접 눈으로 보며 디버깅할 수 있습니다.
    asyncio.run(test_single_horse_image(target_hrno, headless=True))
