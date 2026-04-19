import csv

# 파일 경로 (본인의 디렉토리 환경에 맞게 조정하세요)
input_file = 'data/HRNO_amt.csv'
output_file = 'data/HRNO_amt_clean.csv'

# 원본 읽기(r) 및 정제본 쓰기(w) 동시에 열기
with open(input_file, 'r', encoding='utf-8') as f_in, \
        open(output_file, 'w', encoding='utf-8', newline='') as f_out:
    reader = csv.reader(f_in)
    writer = csv.writer(f_out)

    # 1. 헤더 추출 및 새 파일 1번 행에 정확히 기록
    # (컬럼명이 데이터로 들어가는 오류 방지)
    header = next(reader)
    writer.writerow(header)

    # 2. 2번 행부터 실제 데이터 반복 처리
    for row in reader:
        hrno = row[0]
        amt = row[1].strip()  # 눈에 안 보이는 공백 제거

        # '-'가 아닌 경우에만 콤마와 万円 제거
        if amt != '-':
            amt = amt.replace(',', '').replace('万円', '')

        # 정제된 데이터를 새 파일에 적재
        writer.writerow([hrno, amt])

print("데이터 정제가 완료되어 새 파일로 저장되었습니다.")