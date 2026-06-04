# 단위 테스트 후보 목록

작성일: 2026-06-02
읽기 기준 프로젝트 루트: `C:\Users\비큐리오\PycharmProjects`
쓰기 가능 테스트 루트: `C:\Users\비큐리오\PycharmProjects\tests`

## 요약

이 프로젝트는 Netkeiba 데이터를 수집하고, 파싱하고, DB에 적재하고, 외부 API와 이메일 리포트까지 연결하는 자동화 파이프라인입니다. 단위 테스트 1차 대상은 순수 파싱, 문자열 정규화, SQL 쿼리 생성, CSV 변환, 리포트 파일 생성처럼 외부 부작용 없이 검증 가능한 로직이 좋습니다.

네트워크, VPN, SMTP, Playwright, 실제 MariaDB 연결은 단위 테스트에서는 직접 실행하지 않고 mock 처리하거나 통합 테스트 영역으로 분리하는 편이 안전합니다.

## 스캔 범위

이번 정리는 `C:\Users\비큐리오\PycharmProjects` 아래의 Python 파일 전체 목록을 기준으로 다시 확인했습니다.

확인된 Python 파일 수:

- 전체 Python 파일: 38개
- `tests` 내부 기존 테스트 파일: 2개
- `scratch`와 임시/보조 스크립트: 4개
- 실제 프로젝트 코드와 도구성 스크립트: 32개

확인 방식:

- `rg --files "C:\Users\비큐리오\PycharmProjects" -g "*.py"`로 전체 Python 파일 목록 확인
- `rg -n "^(async\s+def|def|class)\s+" ... -g "*.py"`로 모든 함수/클래스 선언 확인
- `rg -n "^(from|import)\s+" ... -g "*.py"`로 외부 의존성과 부작용 가능성이 큰 모듈 확인

주의:

- 모든 파일의 모든 줄을 정밀 코드리뷰한 것은 아닙니다.
- 단위 테스트 후보 선정을 위해 전체 파일의 구조, 함수 선언, import, 핵심 구현부를 스캔했습니다.
- 우선순위가 높은 파서/DB/리포트 파일은 일부 내용을 직접 읽어서 테스트 후보를 구체화했습니다.

## 파일별 분류

### 루트 스크립트

- `main.py`: 전체 자동화 오케스트레이션입니다. 순수 단위 테스트 후보는 `extract_suffix_from_filename`, `validate_csv_data`, `validate_result_csv_data`, `trigger_external_api(APP_ENV=test)`입니다. 나머지는 subprocess, sleep, 외부 API, 크롤러 실행이 섞여 있어 mock 또는 통합 테스트 성격입니다.
- `netkeiba_auth.py`: Playwright 로그인과 세션 저장 로직입니다. 단위 테스트보다는 브라우저/세션 통합 테스트 대상입니다.
- `fix.py`: `main.py`를 치환하는 일회성 보정 스크립트 성격입니다. 정규식 치환 함수 `replacer` 정도만 테스트 가능하지만 우선순위는 낮습니다.

### WebCrawler

- `src/crawlers/WebCrawler/parser.py`: 결과 페이지 파서입니다. 최우선 단위 테스트 대상입니다.
- `src/crawlers/WebCrawler/main.py`: Playwright로 결과 페이지를 순회하고 CSV를 저장합니다. `make_race_urls`, `save_rows_to_csv`는 단위 테스트 가능하고, 브라우저 실행부는 통합 테스트 대상입니다.
- `src/crawlers/WebCrawler/noncentral.py`: NAR 결과 파서와 CSV 유틸이 함께 있습니다. 파서/URL/CSV 유틸은 단위 테스트 후보입니다.
- `src/crawlers/WebCrawler/discovery.py`: 개최 일정 탐색 로직입니다. `get_upcoming_dates`는 단위 테스트 가능하고, `discover_races`, `get_all_target_races`는 requests를 mock해야 합니다.
- `src/crawlers/WebCrawler/no_divider_from_race_result.py`: 결과 CSV에서 HRNO/JKNO/TRNO를 분리 저장하는 도구입니다. 임시 CSV 기반 단위 테스트 후보입니다.
- `src/crawlers/WebCrawler/entry_sheet_2/parser.py`: 출마표 파서입니다. 최우선 단위 테스트 대상입니다.
- `src/crawlers/WebCrawler/entry_sheet_2/main.py`: 출마표 수집 실행부입니다. requests와 인증 쿠키가 필요하므로 통합 또는 mock 테스트 대상입니다.
- `src/crawlers/WebCrawler/race_plan/parser.py`: API용 경주 계획 파서와 PK 추출입니다. 최우선 단위 테스트 대상입니다.

### HRNOCrawler

- `src/crawlers/HRNOCrawler/parser.py`: 말 상세 페이지 파서입니다. 순수 helper는 최우선 단위 테스트 대상이고, 비동기 fetch 함수들은 aiohttp session을 fake로 둔 테스트가 필요합니다.
- `src/crawlers/HRNOCrawler/main.py`: HRNO 목록을 읽고 비동기 수집 결과를 CSV로 저장합니다. `get_completed_hrnos`, `load_hrno_list_from_csv`, `save_results_to_csv`는 단위 테스트 후보입니다.
- `src/crawlers/HRNOCrawler/image_downloader.py`: Playwright 이미지 다운로드, VPN, 파일 저장이 섞여 있습니다. `load_hrno_list_from_csv`, `log_failed_hrno` 정도만 단위 테스트 후보이고 나머지는 통합 테스트 성격입니다.
- `src/crawlers/HRNOCrawler/check_horse_dirs.py`: 로컬 디렉터리 검사용 도구입니다. 임시 디렉터리 기반 단위 테스트 가능하지만 우선순위는 낮습니다.
- `src/crawlers/HRNOCrawler/check_video_folders.py`: 말 이름별 영상 폴더 검사 도구입니다. 임시 디렉터리와 dataframe fixture로 테스트 가능합니다.
- `src/crawlers/HRNOCrawler/lastamt_fix.py`: requests 기반 보정 스크립트입니다. HTML 파싱 부분을 분리하면 테스트성이 좋아집니다.
- `src/crawlers/HRNOCrawler/hramt_fix.py`: 현재 함수 선언이 없는 보정 스크립트로 보입니다. 단위 테스트보다 리팩토링 후 테스트 대상입니다.
- `src/crawlers/HRNOCrawler/makeone.py`: pandas 기반 보조 스크립트입니다. 우선순위는 낮습니다.

### JKNOCrawler

- `src/crawlers/JKNOCrawler/parser.py`: 기수 프로필/성적 파서입니다. 최우선 단위 테스트 대상입니다.
- `src/crawlers/JKNOCrawler/main.py`: fetch, VPN, CSV append, parser 연결 실행부입니다. CSV 유틸은 단위 테스트 가능하고 네트워크/VPN은 mock 대상입니다.
- `src/crawlers/JKNOCrawler/debug_jockey.py`: 디버그용 스크립트입니다. 우선순위는 낮습니다.
- `src/crawlers/JKNOCrawler/scratch/test_encoding.py`: scratch 테스트성 파일입니다. 정식 단위 테스트 후보에서는 제외합니다.

### TRNOCrwaler

- `src/crawlers/TRNOCrwaler/main.py`: 조교사 result 파서, CSV 유틸, fetch/VPN 실행부가 함께 있습니다. 문자열/HTML 파싱 helper는 높은 우선순위의 단위 테스트 후보입니다.

### InformationCrawler

- `src/crawlers/InformationCrawler/main.py`: 공지/취소/제외/중지 정보 수집, cache, DB 동기화, Telegram/API 호출이 섞여 있습니다. `generate_hash`, `get_rcdate_from_day`, `parse_cancel_record`, CSV 저장은 단위 테스트 후보입니다.
- `src/crawlers/InformationCrawler/scratch/reprocess_old_csv.py`: 과거 CSV 재처리용 스크립트입니다. `get_rcdate_historical` 정도는 단위 테스트 가능하지만 정식 후보 우선순위는 낮습니다.

### WeatherCrawler

- `src/crawlers/WeatherCrawler/main.py`: 날씨/마장 정보 fetch와 CSV 저장입니다. requests를 mock하면 `fetch_weather_and_track` 일부 검증이 가능하지만, 1차 단위 테스트 후보로는 낮은 편입니다. `save_to_csv`는 임시 파일로 테스트 가능합니다.

### Database

- `src/database/DBIntegration/mariadb_upsert.py`: 계획 데이터 CSV upsert입니다. `generate_upsert_query`, fake connection 기반 `process_csv_file`이 좋은 단위 테스트 후보입니다.
- `src/database/DBIntegration/mariadb_result_upsert.py`: 결과 데이터 upsert와 기존 경주 삭제입니다. `generate_upsert_query`, `clean_old_races`, `process_csv_file`이 좋은 단위 테스트 후보입니다.
- `src/database/DBIntegration/mariadb_api_transfer.py`: DB procedure/API용 transfer 실행부입니다. 실제 DB 의존도가 높아 mock 또는 통합 테스트 대상입니다.
- `src/database/DBIntegration/mariadb_result_api_transfer.py`: 결과 transfer 실행부입니다. 실제 DB 의존도가 높아 mock 또는 통합 테스트 대상입니다.

### Reporting

- `src/reporting/Reporting/email_report.py`: 일본 경마 리포트 생성/발송입니다. `decode_mime_words`, `create_excel_file`, mock DB 기반 `generate_hybrid_report`가 단위 테스트 후보입니다.
- `src/reporting/Reporting/email_report_kor.py`: 한국 경마 리포트 생성/발송입니다. 일본 리포트와 같은 방식으로 테스트 가능합니다.

### Common/tools

- `src/common/tools/jmafeel_shared_image_cleaner.py`: 공유 이미지 디렉터리 정리 도구입니다. 파일 삭제/이동 가능성이 있어 테스트 시 임시 디렉터리와 monkeypatch가 필요합니다. 1차 후보로는 낮습니다.

### 기존 tests/scratch

- `tests/test_email.py`: 실제 IMAP 접근 가능성이 있어 단위 테스트보다는 수동/통합 테스트 성격입니다.
- `tests/test_single_image_download.py`: Playwright 이미지 다운로드 테스트로 통합 테스트 성격입니다.
- `scratch/debug_telegram.py`, `scratch/test_telegram_alert.py`: 실제 Telegram 또는 잘못된 import 가능성이 있는 디버그 스크립트입니다. 정식 단위 테스트 후보에서는 제외합니다.

## 최우선 후보

### WebCrawler 결과 파서

대상 파일: `src/crawlers/WebCrawler/parser.py`

추천 테스트 파일: `tests/test_webcrawler_parser.py`

테스트 대상:

- `sanitize_text`: 줄바꿈, 탭, ASCII 제어문자를 제거하고 문자열이 아닌 값은 그대로 유지하는지 확인합니다.
- `parse_cookie_string`: `key=value; key2=value2` 형태 쿠키 문자열을 dict로 바꾸고, dict 입력은 그대로 반환하며, 잘못된 입력은 빈 dict로 처리하는지 확인합니다.
- `parse_race_item02`: `.RaceList_Item02` 영역에서 등급, 날짜, 경주명, 출발 시간, 거리, 날씨, 마장 상태, 장소, 등급 조건, 출전두수, 상금 필드를 추출하는지 확인합니다.
- `parse_race_table01`: 말 목록, 말/기수/조교사 ID, 성별/나이, 부담중량, `dusu` 제한을 처리하는지 확인합니다.
- `parse_premium_lap_summary`: `#lap_summary`에서 말 ID별 구간 기록을 최대 20개 distance 필드로 매핑하는지 확인합니다.
- `parse_race_page_rows`: 경주 메타데이터, 말 목록, 랩타임 정보를 합치고 최종 값을 정리하는지 확인합니다.

단위 테스트에 적합한 이유:

- HTML 문자열 또는 BeautifulSoup 객체만 있으면 테스트할 수 있습니다.
- 실제 네트워크 접근이 필요 없습니다.
- 결과 수집 파이프라인의 핵심 변환 로직입니다.

추천 케이스:

- 필요한 selector가 없을 때 예외 없이 기본 구조를 반환하는지 확인합니다.
- 등급 아이콘 class가 기대한 `RCGRD` 값으로 매핑되는지 확인합니다.
- `race_id=YYYY...` URL에서 연도와 경주번호를 가져오는지 확인합니다.
- 말/기수/조교사 링크 끝의 숫자 ID를 추출하는지 확인합니다.
- 특정 말의 랩타임이 해당 row에 병합되는지 확인합니다.

### WebCrawler 출마표 파서

대상 파일: `src/crawlers/WebCrawler/entry_sheet_2/parser.py`

추천 테스트 파일: `tests/test_entry_sheet_parser.py`

테스트 대상:

- `parse_api_entry_sheet_2`: 계획 경주의 메타데이터, 말 목록, `HRNO`, `JKNO`, `TRNO`, 성별/나이, 부담중량, 배당, 인기, 등급, 조건, 상금 정보를 추출하는지 확인합니다.

단위 테스트에 적합한 이유:

- BeautifulSoup 객체와 선택적 `odds_data` dict만 있으면 테스트할 수 있습니다.
- Netkeiba HTML을 CSV/API용 데이터로 바꾸는 중요한 경계입니다.

추천 케이스:

- odds JSON으로 `WIN_ODDS`와 `POPULARITY`가 채워지는지 확인합니다.
- `セ`가 `セン`으로 정규화되는지 확인합니다.
- span 정보가 부족할 때 경주명에서 `AGECOND`와 `RANK`를 보완하는지 확인합니다.
- `.RaceTable01`이 없으면 빈 list를 반환하는지 확인합니다.

### WebCrawler 경주 계획 파서

대상 파일: `src/crawlers/WebCrawler/race_plan/parser.py`

추천 테스트 파일: `tests/test_race_plan_parser.py`

테스트 대상:

- `parse_api_race_plan`: API용 경주 계획 메타데이터를 추출하는지 확인합니다.
- `parse_pks`: 출마표 row에서 말, 기수, 조교사 PK set을 추출하는지 확인합니다.

추천 케이스:

- URL의 `race_id`가 `RACE_DT`와 `RACE_NO`로 매핑되는지 확인합니다.
- 날짜, 장소, 거리, 방향, 경주 등급, 성별 조건, 출전두수, 상금 필드가 파싱되는지 확인합니다.
- 중복 PK 링크가 set으로 중복 제거되는지 확인합니다.
- 테이블이 없으면 빈 set들을 반환하는지 확인합니다.

### HRNOCrawler 파서 헬퍼

대상 파일: `src/crawlers/HRNOCrawler/parser.py`

추천 테스트 파일: `tests/test_hrno_parser_helpers.py`

테스트 대상:

- `sanitize_text`
- `build_horse_url`
- `_clean_td_value`
- `_extract_no`
- `_extract_html_from_ajax_json`
- `_parse_jp_money`
- `_parse_jp_date`
- `_parse_prize_to_int`

단위 테스트에 적합한 이유:

- 순수 함수가 많고 edge case가 많습니다.
- 더 무거운 비동기 말 상세 파서의 기반 로직입니다.

추천 케이스:

- `1億2345万`, `500万`, `12,345`, `-` 같은 일본식 금액 문자열을 확인합니다.
- `YYYY/MM/DD`, `YYYY.MM.DD`, 비숫자 구분자 날짜, 잘못된 날짜를 확인합니다.
- AJAX JSON의 `html`, `data`, `result`, `body`, `content` 키에서 HTML을 찾는지 확인합니다.

### JKNOCrawler 파서

대상 파일: `src/crawlers/JKNOCrawler/parser.py`

추천 테스트 파일: `tests/test_jkno_parser.py`

테스트 대상:

- `_clean`
- `_extract_number`
- `_split_height_weight_numeric`
- `_parse_birthday_from_name_p`
- `_calc_age`
- `parse_jockey_profile`
- `_to_int_str`
- `_to_float_str_percent_cell`
- `parse_jockey_result_stats`

단위 테스트에 적합한 이유:

- 네트워크 fetch와 파싱 로직이 분리되어 있습니다.
- 작은 HTML fixture로 프로필과 성적 테이블 매핑을 검증할 수 있습니다.

추천 케이스:

- `164cm/53kg`, `身長164cm/体重53kg`, `164 / 53` 같은 신장/체중 변형을 확인합니다.
- 잘못된 생년월일이면 빈 birthday/age를 반환하는지 확인합니다.
- `.ProfileDataTable`이 없을 때도 안정적인 빈 컬럼 구조를 반환하는지 확인합니다.
- 성적 통계에서 통산 row와 2026 row, 승률, 연대율, 합산 출주 수를 추출하는지 확인합니다.

### TRNOCrwaler 파싱 헬퍼

대상 파일: `src/crawlers/TRNOCrwaler/main.py`

추천 테스트 파일: `tests/test_trno_parser_helpers.py`

테스트 대상:

- `norm_text`
- `clean_prname`
- `to_int`
- `parse_name_block`
- `split_p_to_birthday_prgubun`
- `parse_race_table_trs`
- `safe_get`
- `sum_cells_as_int`

단위 테스트에 적합한 이유:

- 대부분 문자열, list, 작은 HTML 조각으로 검증 가능한 순수 헬퍼입니다.
- `fetch_and_map`은 이후 `fetch_html`을 monkeypatch해서 테스트할 수 있습니다.

추천 케이스:

- 괄호가 붙은 조교사 이름이 정리되는지 확인합니다.
- 콤마나 단위가 섞인 숫자 문자열이 int로 변환되는지 확인합니다.
- 테이블 row가 부족할 때 안전하게 `None`을 반환하는지 확인합니다.
- `sum_cells_as_int`가 숫자를 하나도 찾지 못하면 `None`을 반환하는지 확인합니다.

## 중간 우선순위

### NAR/noncentral 결과 파서

대상 파일: `src/crawlers/WebCrawler/noncentral.py`

추천 테스트 파일: `tests/test_noncentral_parser.py`

테스트 대상:

- `get_race_id_from_url`
- `build_url_with_race_id`
- `clean_text_lines`
- `extract_last_token_from_href`
- `parse_sex_age`
- `to_int_like`
- `parse_resultpayback_kv`
- `parse_race_header`
- `parse_corner_pass_table1`
- `parse_result_rows`
- `append_rows_csv`
- `load_existing_ids`
- `save_unique_ids`

메모:

- 파서 테스트는 HTML 조각을 사용하면 됩니다.
- CSV 유틸 테스트는 `tmp_path`를 사용하면 됩니다.
- `parse_race_header`는 현재 `RCDATE`와 `RCDAY`가 하드코딩되어 있습니다. 동적 날짜가 의도라면 이 동작은 리뷰 항목으로 남기는 것이 좋습니다.

### InformationCrawler 취소/제외 파서

대상 파일: `src/crawlers/InformationCrawler/main.py`

추천 테스트 파일: `tests/test_information_crawler_parser.py`

테스트 대상:

- `generate_hash`
- `get_rcdate_from_day`
- `parse_cancel_record`
- `save_csv`
- `save_cancel_csv`

메모:

- `get_rcdate_from_day`는 `datetime.now()`에 의존하므로 module datetime을 monkeypatch하거나 고정 날짜 기준으로 검증해야 합니다.
- DB 동기화, Telegram, 외부 API 호출은 mock 처리하거나 통합 테스트로 넘기는 편이 좋습니다.

### 마스터 파이프라인 헬퍼

대상 파일: `main.py`

추천 테스트 파일: `tests/test_master_pipeline_helpers.py`

테스트 대상:

- `extract_suffix_from_filename`
- `validate_csv_data`
- `validate_result_csv_data`
- `trigger_external_api`의 `APP_ENV=test` 경로

메모:

- `validate_csv_data`, `validate_result_csv_data`는 module directory 상수를 monkeypatch하면 임시 CSV 파일로 테스트할 수 있습니다.
- `trigger_external_api`는 `APP_ENV=test`에서만 테스트하거나 `requests.get`, `threading.Thread`, `time.sleep`을 mock해야 합니다.

### DBIntegration 쿼리 생성과 CSV 업로드 로직

대상 파일:

- `src/database/DBIntegration/mariadb_upsert.py`
- `src/database/DBIntegration/mariadb_result_upsert.py`

추천 테스트 파일: `tests/test_db_integration_units.py`

테스트 대상:

- `generate_upsert_query`
- fake connection/cursor를 사용한 `process_csv_file`
- fake connection/cursor를 사용한 `clean_old_races`

추천 케이스:

- 모든 컬럼이 backtick으로 감싸지고 placeholder 개수가 맞는지 확인합니다.
- 빈 CSV는 `False`를 반환하는지 확인합니다.
- header만 있는 CSV는 `executemany` 없이 `True`를 반환하는지 확인합니다.
- 빈 문자열 cell이 `None`으로 변환되는지 확인합니다.
- 재시도 가능한 MySQL 오류 코드에서 rollback 후 재시도하는지 확인합니다.
- `clean_old_races`가 `YYYYMMDD`를 `YYYY-MM-DD`로 변환하는지 확인합니다.

### 리포팅 헬퍼

대상 파일:

- `src/reporting/Reporting/email_report.py`
- `src/reporting/Reporting/email_report_kor.py`

추천 테스트 파일: `tests/test_reporting_helpers.py`

테스트 대상:

- `decode_mime_words`
- `create_excel_file`
- DB 연결을 mock한 `generate_hybrid_report`

메모:

- `create_excel_file`은 `tmp_path`에 파일을 만들고 openpyxl로 workbook 구조를 확인하면 됩니다.
- `generate_hybrid_report`는 fake connection/cursor로 제어된 DB row를 반환하게 만들 수 있습니다.
- SMTP/IMAP 동작은 단위 테스트에서는 mock 처리하거나 통합 테스트로 분리하는 것이 좋습니다.

## 낮은 우선순위 또는 통합 테스트 성격

다음 항목들은 가치가 있지만 네트워크, Playwright, VPN, 파일 시스템 부작용, 긴 오케스트레이션을 포함하므로 첫 단위 테스트 대상으로는 적합하지 않습니다.

- `netkeiba_auth.py`: Playwright 로그인과 세션 동작
- `src/crawlers/HRNOCrawler/image_downloader.py`: Playwright 이미지 다운로드와 VPN 동작
- `src/crawlers/WeatherCrawler/main.py`: 실시간 날씨 fetch와 CSV 출력
- `src/crawlers/*/main.py` entrypoint: subprocess 오케스트레이션과 실제 크롤러 실행
- `src/database/DBIntegration/*_api_transfer.py`: 실제 DB/API transfer 경로
- `src/reporting/Reporting/*send_report_email`: SMTP 발송과 첨부파일 처리

## 추천 첫 테스트 묶음

1. `tests/test_webcrawler_parser.py`
2. `tests/test_entry_sheet_parser.py`
3. `tests/test_hrno_parser_helpers.py`
4. `tests/test_jkno_parser.py`
5. `tests/test_db_integration_units.py`

이 묶음은 실제 Netkeiba 접속, 인증정보, MariaDB, Telegram, SMTP, Playwright, VPN 없이도 가장 위험한 데이터 변환 로직을 넓게 검증할 수 있습니다.

## 추후 리뷰 관찰 사항

- 여러 모듈이 import 시점에 로그 디렉터리를 만들고 file handler를 설정합니다. 단위 테스트에서 `tests` 밖에 로그가 쓰이지 않도록 import 격리나 logging monkeypatch가 필요할 수 있습니다.
- 일부 모듈은 자기 디렉터리 기준으로 `from parser import ...`를 사용합니다. 테스트에서는 `sys.path` 조정이나 파일 경로 기반 import가 필요할 수 있습니다.
- `src/crawlers/WebCrawler/noncentral.py`의 `parse_race_header`에는 `RCDATE`, `RCDAY`가 하드코딩되어 있습니다. NAR 결과 파서의 의도된 동작인지 확인이 필요합니다.
- `src/crawlers/JKNOCrawler/parser.py`는 성적 통계의 현재 연도 row를 `2026`으로 고정합니다. 매년 바뀌어야 한다면 테스트로 연도 의존성을 드러내고 구현을 설정 가능하게 바꾸는 것이 좋습니다.
- `main.py`와 여러 crawler module은 오케스트레이션, subprocess 호출, sleep, 파싱이 한 파일에 섞여 있습니다. 단위 테스트는 순수 헬퍼부터 시작하고, 오케스트레이션 경계는 monkeypatch로 다루는 편이 좋습니다.
