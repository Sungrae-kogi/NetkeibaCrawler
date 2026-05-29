from pathlib import Path
import pandas as pd


def main() -> None:
    project_root = Path(__file__).resolve().parent

    # -------------------------
    # 1) 입력 경로 / 패턴
    # -------------------------
    input_dir = project_root / "data"
    pattern = "jockey_profile_2025*.csv"
    input_files = sorted(input_dir.glob(pattern))

    # 출력 파일
    output_path = input_dir / "jkno_unique.csv"

    # -------------------------
    # 2) 파일 목록 확인
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
    # 3) JKNO 수집
    # -------------------------
    jkno_list: list[pd.Series] = []
    total_rows = 0

    for i, fp in enumerate(input_files, start=1):

        try:
            # 인코딩 대응
            try:
                df = pd.read_csv(
                    fp,
                    usecols=["JKNO"],
                    dtype={"JKNO": "string"},
                    encoding="utf-8-sig"
                )
            except UnicodeDecodeError:
                df = pd.read_csv(
                    fp,
                    usecols=["JKNO"],
                    dtype={"JKNO": "string"},
                    encoding="cp932"
                )

        except ValueError:
            print(f"[WARN] ({i}) {fp.name}: JKNO column not found. Skip.")
            continue

        except Exception as e:
            print(f"[WARN] ({i}) {fp.name}: Read failed ({e})")
            continue

        # 정리
        s = df["JKNO"].astype("string").str.strip()
        s = s[s.notna() & (s != "")]

        total_rows += len(s)
        jkno_list.append(s)

        # 샘플 출력
        sample = s.head(5).tolist()
        print(f"[DEBUG] ({i}/{len(input_files)}) {fp.name}: rows={len(s)}, sample={sample}")

    if not jkno_list:
        print("[ERROR] No JKNO collected.")
        return

    # -------------------------
    # 4) 합치기 + 중복제거
    # -------------------------
    all_jkno = pd.concat(jkno_list, ignore_index=True)

    unique_jkno = (
        all_jkno
        .drop_duplicates()
        .sort_values()        # 필요 없으면 제거 가능
        .reset_index(drop=True)
    )

    print(f"[INFO] Total rows(before dedup): {len(all_jkno)}")
    print(f"[INFO] Unique JKNO count: {len(unique_jkno)}")
    print(f"[DEBUG] Sample unique: {unique_jkno.head(10).tolist()}")

    # -------------------------
    # 5) 저장
    # -------------------------
    out_df = pd.DataFrame({"JKNO": unique_jkno})
    out_df.to_csv(output_path, index=False, encoding="utf-8-sig")

    print(f"[DONE] Saved: {output_path}")


if __name__ == "__main__":
    main()
