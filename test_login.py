import json
import time
from pathlib import Path
from playwright.sync_api import sync_playwright

def run_login_test():
    # 1. 설정 로드
    config_path = Path(__file__).parent / "config.json"
    if not config_path.exists():
        print(f"❌ config.json 파일이 없습니다: {config_path}")
        return

    with open(config_path, "r", encoding="utf-8") as f:
        config = json.load(f)

    user_id = config.get("NETKEIBA_ID")
    user_pw = config.get("NETKEIBA_PW")
    session_file = config.get("SESSION_FILE", "storage_state.json")

    # 디버깅: 읽어온 아이디의 앞자리만 출력하여 YOUR_ID가 아닌지 확인
    if user_id:
        print(f"🔎 읽어온 아이디 확인: {user_id[:3]}***")

    # 체크 로직 제거

    print("🚀 Playwright 가동 중...")
    with sync_playwright() as p:
        # headed=True로 설정하여 브라우저가 뜨는 것을 볼 수 있게 함
        browser = p.chromium.launch(headless=False)
        context = browser.new_context()
        page = context.new_page()

        print("🌐 메인 페이지 접속 중...")
        page.goto("https://www.netkeiba.com/")
        
        # '로그인' 링크 클릭 (보통 상단에 있음)
        print("🔗 로그인 페이지로 이동 중...")
        # 넷케이바 메인에서 로그인 링크 텍스트 기반 클릭
        page.click("text=ログイン")
        
        # 2. 아이디/비밀번호 입력
        print("⌨️ 계정 정보 입력 중 (지연 시간 추가)...")
        page.wait_for_selector("input[name='login_id']")
        
        # 실제 사람이 입력하는 것처럼 필드를 클릭하고 글자 사이에 딜레이를 줌
        page.click("input[name='login_id']")
        page.type("input[name='login_id']", user_id, delay=100)
        
        page.click("input[name='pswd']")
        page.type("input[name='pswd']", user_pw, delay=100)

        time.sleep(1) # 입력 완료 후 잠시 대기

        # 3. 로그인 버튼 클릭
        print("🖱️ 로그인 버튼 클릭...")
        page.click("input[alt='ログイン']")

        # 4. 결과 대기 및 확인
        print("⏳ 로그인 결과 대기 중 (최대 20초)...")
        try:
            # 1. 성공 케이스: 메인 페이지 이동 또는 로그인 정보 태그 확인
            # (로그인 후에는 보통 My페이지 링크나 로그아웃 버튼이 생김)
            success_selector = "text=로그아웃" # 또는 ログアウト
            
            # 여러 조건을 동시에 기다림
            page.wait_for_function("""
                () => document.body.innerText.includes('로그아웃') || 
                      document.body.innerText.includes('ログアウト') ||
                      document.body.innerText.includes('ID 또는 비밀번호') ||
                      document.body.innerText.includes('IDまたはパスワード')
            """, timeout=20000)

            full_text = page.inner_text("body")
            if "ID 또는 비밀번호" in full_text or "IDまたはパスワード" in full_text:
                print("❌ 로그인 실패: 아이디 또는 비밀번호가 틀렸습니다.")
            elif "로그아웃" in full_text or "ログアウト" in full_text:
                print("✅ 로그인 성공!")
                context.storage_state(path=session_file)
                print(f"💾 세션 정보가 '{session_file}'에 저장되었습니다.")
            else:
                print("⚠️ 로그인 상태를 확신할 수 없습니다. 화면을 확인해 주세요.")
            
        except Exception as e:
            print(f"❌ 대기 중 오류 발생: {e}")
            # 스크린샷 저장하여 상태 확인
            page.screenshot(path="login_error.png")
            print("📸 현재 화면을 'login_error.png'로 저장했습니다. 이 파일을 확인해 주세요.")
            print("💡 수동으로 로그인을 완료하거나 캡차(Captcha)가 있는지 확인해 주세요.")
            time.sleep(30)

        browser.close()

if __name__ == "__main__":
    run_login_test()
