from pathlib import Path
import csv

def check_directories():
    base_dir = Path(r"Z:\JMaFeel\경주마 영상\경주마")
    if not base_dir.exists():
        print(f"디렉토리를 찾을 수 없습니다: {base_dir}")
        return
        
    flagged_horses = []
    
    for horse_dir in base_dir.iterdir():
        if not horse_dir.is_dir():
            continue
            
        horse_name = horse_dir.name
        
        # 1. 마주복색 체크
        has_silk = (horse_dir / "마주복색.jpg").exists()
        
        # 2. 일반 사진 체크 (마주복색.jpg를 제외한 다른 .jpg 파일이 있는지)
        has_images = False
        for f in horse_dir.iterdir():
            if f.is_file() and f.suffix.lower() == ".jpg" and f.name != "마주복색.jpg":
                has_images = True
                break
        
        # 3. 부마/모마 폴더 체크
        has_sire = (horse_dir / "부마").is_dir()
        has_dam = (horse_dir / "모마").is_dir()
        
        reason = []
        if not has_silk:
            reason.append("마주복색 없음")
            
        if not has_images and not has_sire and not has_dam:
            reason.append("사진도 없고 부마/모마 폴더도 둘 다 없음")
            
        if reason:
            flagged_horses.append({
                "HorseName": horse_name,
                "Reason": ", ".join(reason)
            })
            
    # 결과 요약 출력
    print(f"검사 완료! 총 {len(flagged_horses)} 개의 문제가 있는 디렉토리를 찾았습니다.")
    
    # CSV 저장
    out_csv = Path(r"c:\Users\비큐리오\PycharmProjects\logs\missing_horse_images.csv")
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    
    with open(out_csv, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["HorseName", "Reason"])
        writer.writeheader()
        writer.writerows(flagged_horses)
        
    print(f"상세 결과가 CSV 파일로 저장되었습니다: {out_csv}")

if __name__ == "__main__":
    check_directories()
