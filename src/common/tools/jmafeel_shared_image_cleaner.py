import os
import shutil
import logging
from pathlib import Path

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger("ImageCleaner")

def clean_jmafeel_images():
    base_dir = Path(r"Z:\JMaFeel\경주마 영상\경주마")
    
    if not base_dir.exists():
        logger.error(f"❌ 기본 디렉토리를 찾을 수 없습니다: {base_dir}")
        return

    logger.info(f"🔍 탐색 시작: {base_dir}")
    
    cleaned_count = 0
    missing_files_report = []

    # base_dir 안의 모든 하위 디렉토리 순회
    for horse_dir in base_dir.iterdir():
        if not horse_dir.is_dir():
            continue
            
        dir_name = horse_dir.name
        target_png = horse_dir / f"{dir_name}_실사.png"
        
        # 파일명이 '마주복색.jpg' 또는 '이름_마주복색.png' 등 다양할 수 있으므로, '*마주복색.*' 패턴으로 찾습니다.
        silk_files = list(horse_dir.glob("*마주복색.*"))
        target_silk_exists = len(silk_files) > 0
        
        # 파일 누락 여부 검사 (마주복색만 체크)
        if not target_silk_exists:
            logger.warning(f"⚠️ 파일 누락 발견: '{dir_name}' 디렉토리에 마주복색 이미지가 없습니다.")
            missing_files_report.append(f"- {dir_name}: 마주복색 이미지 없음")

        # 디렉토리명_실사.png 가 존재하는 경우 파일 정리 작업 수행
        if target_png.exists():
            logger.info(f"🎯 실사 이미지 확인됨: {dir_name} (정리 작업 시작)")
            
            # 1. 디렉토리명_*.jpg 파일들 삭제 (예: 이름_01.jpg, 이름_02.jpg)
            for jpg_file in horse_dir.glob(f"{dir_name}_*.jpg"):
                try:
                    jpg_file.unlink()
                    logger.info(f"   🗑️ 삭제됨 (파일): {jpg_file.name}")
                except Exception as e:
                    logger.error(f"   ⚠️ 삭제 실패 (파일) - {jpg_file.name}: {e}")
                    
            # 2. '부마', '모마' 디렉토리 삭제
            for sub_folder_name in ["부마", "모마"]:
                sub_folder_path = horse_dir / sub_folder_name
                if sub_folder_path.exists() and sub_folder_path.is_dir():
                    try:
                        shutil.rmtree(sub_folder_path)
                        logger.info(f"   🗑️ 삭제됨 (폴더): {sub_folder_name}")
                    except Exception as e:
                        logger.error(f"   ⚠️ 삭제 실패 (폴더) - {sub_folder_name}: {e}")
                        
            cleaned_count += 1

    logger.info(f"✅ 정리 작업 완료. 총 {cleaned_count}개의 경주마 디렉토리가 정리되었습니다.")
    
    # 누락 파일 최종 요약 리포트 출력
    if missing_files_report:
        print("\n" + "="*50)
        print("📋 [누락 파일 요약 리포트]")
        print("="*50)
        for report in missing_files_report:
            print(report)
        print("-" * 50)
        print(f"총 {len(missing_files_report)}마리의 경주마 폴더에서 파일 누락이 확인되었습니다.")
        print("="*50 + "\n")

if __name__ == "__main__":
    clean_jmafeel_images()