import json
import time
from pathlib import Path
from playwright.sync_api import sync_playwright

BASE_DIR = Path(__file__).resolve().parent
CONFIG_PATH = BASE_DIR / "config" / "config.json"
SESSION_PATH = BASE_DIR / "storage_state.json"          # Session 쿠키 저장.

def get_netkeiba_cookies(force_login=True):
    """
    저장된 세션 파일에서 쿠키를 읽어와 requests에서 사용할 수 있는 형식으로 반환합니다.
    force_login이 True이거나 세션 파일이 없으면 항상 새로 자동 로그인을 수행합니다.
    """
    if force_login or not SESSION_PATH.exists():
        print("🔑 최신 세션 확보를 위해 자동 로그인을 강제 실행합니다...")
        run_auto_login(headless=False)
    
    try:
        with open(SESSION_PATH, "r", encoding="utf-8") as f:
            storage_state = json.load(f)
        
        # Playwright storage_state 구조에서 cookies만 추출하여 requests 형식으로 변환
        cookies = {}
        for cookie in storage_state.get("cookies", []):
            cookies[cookie["name"]] = cookie["value"]
            
        return cookies
    except Exception as e:
        print(f"❌ 세션 로드 중 오류 발생: {e}")
        # 오류 시 1회에 한해 다시 강제 로그인 시도
        run_auto_login(headless=False)
        try:
            with open(SESSION_PATH, "r", encoding="utf-8") as f:
                storage_state = json.load(f)
            cookies = {}
            for cookie in storage_state.get("cookies", []):
                cookies[cookie["name"]] = cookie["value"]
            return cookies
        except Exception as ex:
            print(f"❌ 최종 세션 확보 실패: {ex}")
            return {}

def run_auto_login(headless=False):
    """Playwright를 사용하여 자동 로그인을 수행하고 세션을 저장합니다."""
    if not CONFIG_PATH.exists():
        raise FileNotFoundError("❌ config.json 파일이 없습니다. 설정을 먼저 완료해 주세요.")

    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        config = json.load(f)

    user_id = config.get("NETKEIBA_ID")
    user_pw = config.get("NETKEIBA_PW")

    with sync_playwright() as p:
        # 화면 잠금 상태나 백그라운드 구동을 대비해 디폴트 headless=True로 띄웁니다.
        browser = p.chromium.launch(headless=headless) 
        context = browser.new_context()
        page = context.new_page()

        try:
            print(f"🌐 넷케이바 접속 및 로그인 시도 중... (headless={headless})")
            page.goto("https://www.netkeiba.com/")
            page.click("text=ログイン")
            
            page.wait_for_selector("input[name='login_id']")
            page.type("input[name='login_id']", user_id, delay=50)
            page.type("input[name='pswd']", user_pw, delay=50)
            page.click("input[alt='ログイン']")
            
            print("⏳ 로그인 처리 중... (완료를 확인합니다. 최대 60초 대기)")
            
            # 로그인 폼이 화면에서 사라질 때까지 대기 (성공적으로 넘어갔음을 의미)
            page.wait_for_selector("input[name='login_id']", state="hidden", timeout=60000)
            
            # 안전하게 네트워크 요청이 잦아들 때까지 추가 대기
            page.wait_for_load_state("networkidle", timeout=15000)
            time.sleep(2) # 쿠키 기록용 여유 시간
            
            print("✅ 자동 로그인 절차 완료 (강제 저장)")
            context.storage_state(path=str(SESSION_PATH))
            print(f"💾 세션 파일 저장 완료: {SESSION_PATH}")
            
        except Exception as e:
            print(f"❌ 자동 로그인 실패: {e}")
            # 실패 시 디버깅을 위해 스크린샷 저장
            try:
                page.screenshot(path="auth_error.png")
            except:
                pass
            raise e
        finally:
            browser.close()

def cleanup_session():
    """사용이 완료된 세션 파일(storage_state.json)을 디스크에서 삭제합니다."""
    try:
        if SESSION_PATH.exists():
            SESSION_PATH.unlink()
            print(f"🧹 세션 파일을 안전하게 삭제했습니다: {SESSION_PATH}")
    except Exception as e:
        print(f"⚠️ 세션 파일 삭제 중 요류 발생: {e}")

if __name__ == "__main__":
    # 단독 실행 시 테스트용 (동작 화면을 보려면 headless=False로 설정 가능)
    try:
        cookies = get_netkeiba_cookies(force_login=True)
        print(f"🍪 획득한 쿠키 개수: {len(cookies)}")
    finally:
        cleanup_session()

