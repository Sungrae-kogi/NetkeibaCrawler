import sys
import os
from pathlib import Path

# 현재 파일의 위치를 기준으로 프로젝트 루트 디렉토리를 찾습니다.
# scratch 폴더 안에 있으므로 부모의 부모가 루트입니다.
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(BASE_DIR))

# all.py에서 전송 함수를 가져옵니다.
from all import send_telegram_message

def run_dry_run_test():
    print("="*50)
    print("🚀 텔레그램 알림 테스트(Dry Run)를 시작합니다.")
    print("실제 데이터베이스나 크롤링 작업은 수행하지 않습니다.")
    print("="*50 + "\n")

    try:
        # 1. 수집 단계 테스트
        print("▶ [1/4] 메시지 전송 중: 수집 완료...")
        send_telegram_message("📢 [테스트] 1단계: 모든 데이터를 확실히 CSV로 저장 완료! (Dry Run)")
        
        # 2. DB 적재 단계 테스트
        print("▶ [2/4] 메시지 전송 중: DB 적재 완료...")
        send_telegram_message("📢 [테스트] 20260502 東京: CSV를 DB tmp 테이블에 적재 완료! (Dry Run)")
        
        # 3. 이관 단계 테스트
        print("▶ [3/4] 메시지 전송 중: API 이관 완료...")
        send_telegram_message("📢 [테스트] 20260502 東京: tmp 데이터를 API 테이블에 완벽히 이관 완료! (Dry Run)")
        
        # 4. 외부 API 호출 테스트
        print("▶ [4/4] 메시지 전송 중: API 호출 성공...")
        send_telegram_message("📢 [테스트] 20260502 東京: 마지막 단계: API 호출 성공 및 OK 수신! (Dry Run)")

        print("\n✅ 모든 테스트 메시지를 전송 시도했습니다.")
        print("💡 휴대폰 텔레그램을 확인해 보세요! 알림이 오지 않는다면 config.json의 토큰과 ID를 확인해 주세요.")
    except Exception as e:
        print(f"\n❌ 테스트 중 오류 발생: {e}")

if __name__ == "__main__":
    run_dry_run_test()
