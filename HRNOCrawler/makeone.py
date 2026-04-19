from pathlib import Path
import pandas as pd


def main() -> None:
    project_root = Path(__file__).resolve().parent

    # -------------------------
    # 1) 입력 경로 / 패턴
    # -------------------------
    input_dir = project_root / "data"
    pattern = "horse_profile_*.csv"
    input_files = sorted(input_dir.glob(pattern))

    # 출력 파일
    output_path = input_dir / "hrno_unique.csv"

    # -------------------------
    # 2) 파일 목록 디버그
    # -------------------------
    print(f"[INFO] Input dir: {input_dir}")
    print(f"[INFO] Pattern: {pattern}")
    print(f"[INFO] Matched files: {len(input_files)}")

    if not input_files:
        print("[ERROR] No matched files.")
        return

    for fp in input_files[:10]:
        print(f"  - {fp.name}")
    if len(input_files) > 10:
        print(f"  ... (+{len(input_files) - 10} more)")

    # -------------------------
    # 3) HR_NO 수집
    # -------------------------
    hrno_list: list[pd.Series] = []
    total_rows = 0

    for i, fp in enumerate(input_files, start=1):

        try:
            # 인코딩 대응
            try:
                df = pd.read_csv(
                    fp,
                    usecols=["HR_NO"],
                    dtype={"HR_NO": "string"},
                    encoding="utf-8-sig"
                )
            except UnicodeDecodeError:
                df = pd.read_csv(
                    fp,
                    usecols=["HR_NO"],
                    dtype={"HR_NO": "string"},
                    encoding="cp932"
                )

        except ValueError:
            print(f"[WARN] ({i}) {fp.name}: HR_NO column not found. Skip.")
            continue

        except Exception as e:
            print(f"[WARN] ({i}) {fp.name}: Read failed ({e})")
            continue

        # 정리
        s = df["HR_NO"].astype("string").str.strip()
        s = s[s.notna() & (s != "")]

        total_rows += len(s)
        hrno_list.append(s)

        # 샘플 출력
        sample = s.head(5).tolist()
        print(f"[DEBUG] ({i}/{len(input_files)}) {fp.name}: rows={len(s)}, sample={sample}")

    if not hrno_list:
        print("[ERROR] No HR_NO collected.")
        return

    # -------------------------
    # 4) 합치기 + 중복제거
    # -------------------------
    all_hrno = pd.concat(hrno_list, ignore_index=True)

    unique_hrno = (
        all_hrno
        .drop_duplicates()
        .sort_values()          # 필요 없으면 제거 가능
        .reset_index(drop=True)
    )

    print(f"[INFO] Total rows(before dedup): {len(all_hrno)}")
    print(f"[INFO] Unique HRNO count: {len(unique_hrno)}")
    print(f"[DEBUG] Sample unique: {unique_hrno.head(10).tolist()}")

    # -------------------------
    # 5) 저장 (컬럼명 HRNO로 변경)
    # -------------------------
    out_df = pd.DataFrame({"HRNO": unique_hrno})
    out_df.to_csv(output_path, index=False, encoding="utf-8-sig")

    print(f"[DONE] Saved: {output_path}")


if __name__ == "__main__":
    main()
