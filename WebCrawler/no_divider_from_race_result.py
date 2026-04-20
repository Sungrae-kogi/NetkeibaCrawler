import csv
from pathlib import Path


def extract_and_save_ids(input_files):
    base_dir = Path(__file__).resolve().parent.parent
    targets = {
        "HRNO": base_dir / "HRNOCrawler" / "nodata",
        "JKNO": base_dir / "JKNOCrawler" / "nodata",
        "TRNO": base_dir / "TRNOCrwaler" / "nodata"
    }

    for input_file in input_files:
        file_path = Path(input_file)
        
        # 만약 파일이 현재 경로에 없다면 data/ 폴더 안을 확인
        if not file_path.exists():
            file_path = Path(__file__).resolve().parent / "data" / input_file
            
        if not file_path.exists():
            print(f"▶ 파일이 없습니다: {input_file}")
            continue
            
        name = file_path.stem
        if name.startswith("race_planning_"):
            suffix = name[len("race_planning_"):]
        else:
            suffix = "unknown_date"

        data_sets = {
            "HRNO": set(),
            "JKNO": set(),
            "TRNO": set()
        }

        with open(file_path, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                for key in targets:
                    val = row.get(key)
                    if val and val.strip():
                        data_sets[key].add(val.strip())

        for key, folder_path in targets.items():
            out_dir = Path(folder_path)
            out_dir.mkdir(parents=True, exist_ok=True)
            out_file = out_dir / f"{key}_{suffix}_list.csv"

            with open(out_file, 'w', newline='', encoding='utf-8-sig') as f:
                writer = csv.writer(f)
                writer.writerow([key])
                for item in sorted(data_sets[key]):
                    writer.writerow([item])

        print(
            f"분배 완료: "
            f"HRNO({len(data_sets['HRNO'])}건), "
            f"JKNO({len(data_sets['JKNO'])}건), "
            f"TRNO({len(data_sets['TRNO'])}건)"
        )

import sys
if __name__ == "__main__":
    if len(sys.argv) > 1:
        files_to_process = [sys.argv[1]]
    else:
        files_to_process = ["race_nak.csv"]
    extract_and_save_ids(files_to_process)