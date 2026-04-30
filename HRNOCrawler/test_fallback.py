import asyncio
from image_downloader import download_horse_images

async def test_single_horse_fallback():
    # 테스트 대상: 마운테ンド터 (2023107464) - 현재 사진 0장인 상태
    target_hrno = "2023107464"
    print(f"🚀 테스트 시작: HRNO {target_hrno} (마운테ンド터 - 백업 테스트)")

    
    await download_horse_images(target_hrno, max_images=30)
    
    print("\n✅ 테스트 종료. Z드라이브 폴더를 확인해 주세요.")

if __name__ == "__main__":
    asyncio.run(test_single_horse_fallback())
