import os
import argparse
import pandas as pd

def check_video_folders(horse_names):
    video_dir = r"Z:\JMaFeel\경주마 영상\경주마"
    
    missing_folders = []
    found_folders = []
    
    if not os.path.exists(video_dir):
        print(f"[오류] 영상 기본 경로를 찾을 수 없습니다: {video_dir}")
        return
        
    for hname in horse_names:
        hname = str(hname).strip()
        if not hname or hname == 'nan':
            continue
            
        folder_path = os.path.join(video_dir, hname)
        if os.path.exists(folder_path) and os.path.isdir(folder_path):
            found_folders.append(hname)
        else:
            missing_folders.append(hname)
            
    print(f"========== 영상 폴더 확인 결과 ==========")
    print(f"조회 대상: {len(horse_names)}마리")
    print(f"존재하는 폴더: {len(found_folders)}개")
    print(f"누락된 폴더: {len(missing_folders)}개")
    
    if missing_folders:
        print("\n[누락된 경주마 목록]")
        for m in sorted(missing_folders):
            print(f"- {m}")
    print("=========================================")

def main():
    parser = argparse.ArgumentParser(description="경주마 영상 폴더 누락 확인 스크립트")
    parser.add_argument("-n", "--names", nargs='+', help="확인할 경주마 이름 목록 (공백으로 구분)")
    parser.add_argument("-f", "--file", help="확인할 경주마 이름이 있는 텍스트 파일(엔터 구분) 또는 CSV 파일(HRNAME 컬럼 기준)")
    
    args = parser.parse_args()
    horse_names = set()
    
    # 1. 인자로 이름 리스트를 직접 넘긴 경우
    if args.names:
        horse_names.update(args.names)
        
    # 2. 파일로 넘긴 경우
    if args.file:
        file_path = args.file
        if not os.path.exists(file_path):
            print(f"[오류] 파일을 찾을 수 없습니다: {file_path}")
            return
            
        if file_path.lower().endswith('.csv'):
            try:
                df = pd.read_csv(file_path)
                if 'HRNAME' in df.columns:
                    names = df['HRNAME'].dropna().unique().tolist()
                elif 'HRNO' not in df.columns and len(df.columns) == 1:
                    names = df.iloc[:, 0].dropna().unique().tolist()
                else:
                    print("[오류] CSV 파일에서 HRNAME 컬럼을 찾을 수 없거나 형식이 맞지 않습니다.")
                    return
                horse_names.update(names)
            except Exception as e:
                print(f"[오류] CSV 파일 읽기 실패: {e}")
        else:
            with open(file_path, 'r', encoding='utf-8-sig') as f:
                for line in f:
                    name = line.strip()
                    if name:
                        horse_names.add(name)
                        
    if not horse_names:
        print("사용법 예시:")
        print("  python check_video_folders.py -n 말1 말2 말3")
        print("  python check_video_folders.py -f api_entry_sheet_2_東京_20260509.csv")
        print("  python check_video_folders.py -f my_horses.txt")
        return
        
    check_video_folders(list(horse_names))

if __name__ == "__main__":
    main()
