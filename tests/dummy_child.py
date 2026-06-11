import sys
import time

def main():
    print(f"[{sys.argv[0]}] 더미 자식 프로세스 시작...")
    # 임의의 지연을 주어 실제 네트워크 딜레이 표현
    time.sleep(1.0)
    print(f"[{sys.argv[0]}] 일부 경주 데이터 누락 발생! (exit code 2 반환)")
    sys.exit(2)

if __name__ == "__main__":
    main()
