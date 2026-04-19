import os
import sys
import subprocess
from pathlib import Path

# Paths Setup
BASE_DIR = Path(__file__).resolve().parent
WEB_CRAWLER_DIR = BASE_DIR / "WebCrawler"
ENTRY_SHEET_DIR = WEB_CRAWLER_DIR / "entry_sheet_2"
HR_DIR = BASE_DIR / "HRNOCrawler"
JK_DIR = BASE_DIR / "JKNOCrawler"
TR_DIR = BASE_DIR / "TRNOCrwaler"

def print_menu():
    print("=" * 60)
    print("   🐎 넷케이바 (Netkeiba) 기가 크롤링 마스터 파이프라인 🐎")
    print("=" * 60)
    print("크롤링 모드를 선택하세요:")
    print("  1. 과거 경기 결과 수집 모드 (result)")
    print("  2. 미래 경기 계획 수집 모드 (shutuba/entry_sheet)")
    print("=" * 60)

def extract_suffix_from_filename(csv_path: Path, prefix: str) -> str:
    if not csv_path.exists():
        return "unknown"
    stem = csv_path.stem
    if stem.startswith(prefix):
        return stem[len(prefix):]
    return "unknown"

def run_child_crawlers(date_str: str):
    print(f"\n========== [Phase 3] 하위 디테일 크롤러 연쇄 가동 (날짜/장소: {date_str}) ==========")
    
    # 1. HRNOCrawler
    print(f"\n▶ [1/3] HRNOCrawler (말 상세 프로필) 가동 중...")
    if not (HR_DIR / "main.py").exists():
        print(f"[경고] {HR_DIR}/main.py 파일을 찾을 수 없어 건너뜁니다.")
    else:
        subprocess.run([sys.executable, "main.py", date_str], cwd=HR_DIR)
    
    # 2. JKNOCrawler
    print(f"\n▶ [2/3] JKNOCrawler (기수 상세 프로필) 가동 중...")
    if not (JK_DIR / "main.py").exists():
        print(f"[경고] {JK_DIR}/main.py 파일을 찾을 수 없어 건너뜁니다.")
    else:
        subprocess.run([sys.executable, "main.py", date_str], cwd=JK_DIR)
    
    # 3. TRNOCrwaler
    print(f"\n▶ [3/3] TRNOCrwaler (조교사 상세 프로필) 가동 중...")
    if not (TR_DIR / "main.py").exists():
        print(f"[경고] {TR_DIR}/main.py 파일을 찾을 수 없어 건너뜁니다.")
    else:
        subprocess.run([sys.executable, "main.py", date_str], cwd=TR_DIR)
    
    print("\n========== [완료] 전체 마스터 파이프라인 수집이 종료되었습니다! ==========")

def run_mode_1(url: str):
    print("\n========== [Phase 1] 경기 결과 웹 크롤링 수집 시작 ==========")
    subprocess.run([sys.executable, "main.py", url], cwd=WEB_CRAWLER_DIR)
    
    print("\n========== [Phase 2] PK (고유번호) 분배 및 저장 중 ==========")
    csv_files = list(WEB_CRAWLER_DIR.glob("race_planning_*.csv"))
    if not csv_files:
        print("[오류] race_planning CSV 파일을 찾을 수 없습니다.")
        return
        
    latest_csv = max(csv_files, key=lambda p: p.stat().st_mtime)
    subprocess.run([sys.executable, "no_divider_from_race_result.py", latest_csv.name], cwd=WEB_CRAWLER_DIR)
    
    suffix = extract_suffix_from_filename(latest_csv, "race_planning_")
    run_child_crawlers(suffix)

def run_mode_2(url: str):
    print("\n========== [Phase 1 & 2] 경기 계획 수집 및 자동 PK 분배 시작 ==========")
    subprocess.run([sys.executable, "main.py", url], cwd=ENTRY_SHEET_DIR)
    
    csv_files = list((ENTRY_SHEET_DIR / "data").glob("api_entry_sheet_2_*.csv"))
    if not csv_files:
        print("[오류] api_entry_sheet_2 CSV 파일을 찾을 수 없습니다.")
        return
        
    latest_csv = max(csv_files, key=lambda p: p.stat().st_mtime)
    suffix = extract_suffix_from_filename(latest_csv, "api_entry_sheet_2_")
    run_child_crawlers(suffix)

def main():
    print_menu()
    mode = input("선택하실 번호를 입력하세요 (1 또는 2): ").strip()
    
    if mode not in ["1", "2"]:
        print("\n[오류] 잘못된 입력입니다. 프로그램을 종료합니다.")
        return
        
    print("\n[안내] 자동으로 접근하는 것을 막고 보안을 뚫기 위해, 수동 URL 복사가 필요합니다.")
    url = input("시작하실 1경기 웹페이지의 URL 주소를 통째로 복사해서 붙여넣고 엔터를 치세요:\n> ").strip()
    
    if not url:
        print("\n[오류] URL이 입력되지 않았습니다.")
        return

    if mode == "1":
        run_mode_1(url)
    elif mode == "2":
        run_mode_2(url)

if __name__ == "__main__":
    main()
