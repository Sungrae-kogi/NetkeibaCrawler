import requests
import json
from pathlib import Path

def debug_telegram():
    config_path = Path("config.json")
    with open(config_path, "r", encoding="utf-8") as f:
        config = json.load(f)
    
    token = config.get("TELEGRAM_BOT_TOKEN")
    chat_id = config.get("TELEGRAM_CHAT_ID")
    
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": "디버깅 테스트 메시지입니다!"}
    
    print(f"📡 텔레그램 서버로 요청을 보냅니다...")
    try:
        response = requests.post(url, json=payload, timeout=10)
        print(f"상태 코드: {response.status_code}")
        print(f"서버 응답: {response.text}")
        
        if response.status_code == 200:
            print("\n✅ 서버에서는 성공했다고 합니다! 휴대폰 알림 설정을 확인해 보세요.")
        else:
            print("\n❌ 서버에서 거절되었습니다. 응답 내용을 확인해 보세요.")
    except Exception as e:
        print(f"\n❌ 네트워크 오류 발생: {e}")

if __name__ == "__main__":
    debug_telegram()
