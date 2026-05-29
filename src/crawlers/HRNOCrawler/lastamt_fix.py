import os
import csv
import time
import requests
from bs4 import BeautifulSoup

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_FILE = os.path.join(BASE_DIR, 'data', 'HRNO.csv')
OUTPUT_FILE = os.path.join(BASE_DIR, 'data', 'HRNO_amt.csv')


def get_last_amt(hrno):
    """특정 HRNO의 세리 거래 가격을 크롤링하여 반환"""
    url = f"https://db.netkeiba.com/horse/{hrno}/"
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/114.0.0.0 Safari/537.36"
        )
    }

    try:
        res = requests.get(url, headers=headers, timeout=10)
        res.raise_for_status()

        # [핵심] 일본어 모지바케(글자 깨짐) 완벽 방어
        res.encoding = 'euc-jp'

        soup = BeautifulSoup(res.text, 'html.parser')
        prof_area = soup.select_one('.db_prof_area_02 table')

        if not prof_area:
            return None

        for row in prof_area.find_all('tr'):
            th = row.find('th')

            if th and 'セリ取引価格' in th.text:
                td = row.find('td')
                if td:
                    # 불필요한 span 태그 걷어내기
                    for span in td.find_all('span'):
                        span.decompose()

                    raw_text = td.text.strip()
                    # 하이픈(-) 기호 원형 보존
                    return "-" if raw_text == "-" else raw_text

        return None

    except Exception as e:
        print(f"\n[{hrno}] 크롤링 요청 중 에러 발생: {e}")
        return None


def main():
    if not os.path.exists(INPUT_FILE):
        print(f"에러: {INPUT_FILE} 파일을 찾을 수 없습니다.")
        return

    print("전체 데이터 크롤링 파이프라인을 가동합니다...")

    # utf-8-sig로 원본의 숨은 문자(BOM) 제거, newline=''으로 빈 줄 방지
    with open(INPUT_FILE, 'r', encoding='utf-8-sig') as f_in, \
            open(OUTPUT_FILE, 'w', encoding='utf-8', newline='') as f_out:

        reader = csv.reader(f_in)
        writer = csv.writer(f_out)

        # 1. 대상 CSV에 헤더 명시적 작성
        writer.writerow(['HRNO', 'HR_LAST_AMT'])

        # 2. 원본 CSV의 첫 번째 줄(헤더) 무조건 스킵
        next(reader, None)

        count = 0
        for row in reader:
            if not row or not row[0].strip():
                continue

            hrno = row[0].strip()
            print(f"[{hrno}] 데이터 추출 중...", end=" ")

            # 크롤링 수행
            last_amt = get_last_amt(hrno)

            if last_amt is not None:
                print(f"성공 -> {last_amt}")
                writer.writerow([hrno, last_amt])
            else:
                print("실패 또는 데이터 없음")
                writer.writerow([hrno, ''])

            # [고도화] 데이터 유실 방지를 위한 디스크 즉시 쓰기
            f_out.flush()

            # [고도화] IP 차단 방지를 위한 정중한 딜레이 (1.5초)
            time.sleep(1.5)
            count += 1

    print(f"\n총 {count}건의 크롤링 및 저장 작업이 안전하게 완료되었습니다!")


if __name__ == "__main__":
    main()