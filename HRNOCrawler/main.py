import csv
import random
import asyncio
import aiohttp
from pathlib import Path

from parser import build_horse_url, parse_horse_page

def get_completed_hrnos(out_path: Path) -> set[str]:
    if not out_path.exists():
        return set()

    completed = set()
    with open(out_path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            val = row.get("HR_NO")
            if val:
                completed.add(val.strip())
    return completed

def load_hrno_list_from_csv(
        csv_path: Path,
        col_name: str = "HRNO"
) -> list[str]:
    hrnos = []
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            v = (row.get(col_name) or "").strip()
            if v:
                hrnos.append(v)

    seen = set()
    uniq = []
    for x in hrnos:
        if x not in seen:
            seen.add(x)
            uniq.append(x)
    return uniq

async def fetch_single_horse(
        hrno: str,
        idx: int,
        total: int,
        session: aiohttp.ClientSession,
        sem: asyncio.Semaphore,
        lock: asyncio.Lock,
        out_path: Path,
        meet_name: str
) -> None:
    async with sem:
        url = build_horse_url(hrno)
        try:
            delay = random.uniform(1.0, 2.0)
            await asyncio.sleep(delay)

            data = await parse_horse_page(url, hrno, session=session)
            new_data = {"MEET": meet_name}
            new_data.update(data)

            async with lock:
                file_exists = out_path.exists()
                with open(out_path, "a", encoding="utf-8-sig", newline="") as f:
                    writer = csv.DictWriter(f, fieldnames=list(new_data.keys()))
                    if not file_exists:
                        writer.writeheader()
                    writer.writerow(new_data)
            print(f"[{idx}/{total}] OK HRNO={hrno}")

        except Exception as e:
            print(f"[{idx}/{total}] FAIL HRNO={hrno} / {e}")

async def run_async(
        hrno_list: list[str],
        out_path: Path,
        meet_name: str
) -> None:
    total = len(hrno_list)
    sem = asyncio.Semaphore(3)
    lock = asyncio.Lock()

    async with aiohttp.ClientSession() as session:
        tasks = []
        for idx, hrno in enumerate(hrno_list, start=1):
            task = asyncio.create_task(
                fetch_single_horse(
                    hrno, idx, total, session, sem, lock, out_path, meet_name
                )
            )
            tasks.append(task)
        await asyncio.gather(*tasks)

def save_results_to_csv(results: list[dict], out_path: Path):
    if not results:
        print("[WARN] 저장할 데이터가 없습니다.")
        return
    fieldnames = list(results[0].keys())
    with open(out_path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)
    print(f"[OK] CSV 저장 완료: {out_path}")

import sys

if __name__ == "__main__":
    base_dir = Path(__file__).resolve().parent
    date_suffix = sys.argv[1] if len(sys.argv) > 1 else "unknown"
    meet_name = date_suffix.split('_')[0] if '_' in date_suffix else "unknown"
    
    hrno_csv = base_dir / "nodata" / f"HRNO_{date_suffix}_list.csv"
    
    out_dir = base_dir / "data"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_csv = out_dir / f"HRNO_result_{date_suffix}.csv"

    if not hrno_csv.exists():
        raise FileNotFoundError(f"CSV 없음: {hrno_csv}")

    all_hrnos = load_hrno_list_from_csv(hrno_csv, col_name="HRNO")
    completed_set = get_completed_hrnos(out_csv)
    target_hrnos = [h for h in all_hrnos if h not in completed_set]

    print(f"전체 명단: {len(all_hrnos)} 건")
    print(f"이미 완료: {len(completed_set)} 건")
    print(f"진행 대상: {len(target_hrnos)} 건")

    if not target_hrnos:
        print("🎉 모든 크롤링이 이미 완료되었습니다!")
    else:
        asyncio.run(run_async(target_hrnos, out_csv, meet_name))
        print(f"🎉 크롤링 종료! 결과 파일: {out_csv}")