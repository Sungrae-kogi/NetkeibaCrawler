import json
import time
from pathlib import Path
from playwright.sync_api import sync_playwright

BASE_DIR = Path(__file__).resolve().parent
CONFIG_PATH = BASE_DIR / "config.json"
SESSION_PATH = BASE_DIR / "storage_state.json"

def get_netkeiba_cookies():
    """
    저장된 세션 파일에서 쿠키를 읽어와 requests에서 사용할 수 있는 형식으로 반환합니다.
    세션이 없거나 문제가 있으면 자동 로그인을 수행합니다.
    """
    if not SESSION_PATH.exists():
        print("🔑 세션 파일이 없습니다. 자동 로그인을 시도합니다...")
        run_auto_login()
    
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
        run_auto_login()
        return get_netkeiba_cookies()

def run_auto_login():
    """Playwright를 사용하여 자동 로그인을 수행하고 세션을 저장합니다."""
    if not CONFIG_PATH.exists():
        raise FileNotFoundError("❌ config.json 파일이 없습니다. 설정을 먼저 완료해 주세요.")

    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        config = json.load(f)

    user_id = config.get("NETKEIBA_ID")
    user_pw = config.get("NETKEIBA_PW")

    with sync_playwright() as p:
        # 확인을 위해 headless=False로 설정하여 브라우저 창이 뜨게 합니다.
        browser = p.chromium.launch(headless=False) 
        context = browser.new_context()
        page = context.new_page()

        try:
            print("🌐 넷케이바 접속 및 로그인 시도 중...")
            page.goto("https://www.netkeiba.com/")
            page.click("text=ログイン")
            
            page.wait_for_selector("input[name='login_id']")
            page.type("input[name='login_id']", user_id, delay=50)
            page.type("input[name='pswd']", user_pw, delay=50)
            page.click("input[alt='ログイン']")
            
            # 로그인 버튼 클릭 후 충분한 대기 시간 부여
            print("⏳ 로그인 처리 및 쿠키 저장 대기 중 (5초)...")
            time.sleep(5)
            
            print("✅ 자동 로그인 절차 완료 (강제 저장)")
            context.storage_state(path=str(SESSION_PATH))
            print(f"💾 세션 파일 저장 완료: {SESSION_PATH}")
            
        except Exception as e:
            print(f"❌ 자동 로그인 실패: {e}")
            # 실패 시 디버깅을 위해 스크린샷 저장
            page.screenshot(path="auth_error.png")
            raise e
        finally:
            browser.close()

if __name__ == "__main__":
    # 단독 실행 시 테스트용
    cookies = get_netkeiba_cookies()
    print(f"🍪 획득한 쿠키 개수: {len(cookies)}")
