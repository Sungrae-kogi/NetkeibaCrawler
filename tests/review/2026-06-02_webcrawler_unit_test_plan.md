# WebCrawler 단위 테스트 작성 계획서

작성일: 2026-06-02
대상 범위: `src/crawlers/WebCrawler`
작성 위치: `tests`

## 목적

이번 계획의 목적은 WebCrawler 영역에서 외부 사이트 접속, Playwright 실행, 실제 파일 배포 없이 검증 가능한 단위 테스트를 먼저 만드는 것입니다.

1차 테스트는 HTML 파서와 작은 유틸 함수에 집중합니다. 크롤러 실행부, 인증 쿠키, 네트워크 요청, 브라우저 실행은 이후 mock 테스트 또는 통합 테스트로 분리합니다.

## 대상 파일

WebCrawler 아래 Python 파일은 다음 8개입니다.

- `src/crawlers/WebCrawler/parser.py`
- `src/crawlers/WebCrawler/main.py`
- `src/crawlers/WebCrawler/discovery.py`
- `src/crawlers/WebCrawler/no_divider_from_race_result.py`
- `src/crawlers/WebCrawler/noncentral.py`
- `src/crawlers/WebCrawler/entry_sheet_2/parser.py`
- `src/crawlers/WebCrawler/entry_sheet_2/main.py`
- `src/crawlers/WebCrawler/race_plan/parser.py`

## 1차 단위 테스트 범위

### 1. 결과 페이지 파서

대상: `src/crawlers/WebCrawler/parser.py`

추천 테스트 파일: `tests/test_webcrawler_parser.py`

테스트 대상:

- `sanitize_text`
- `parse_cookie_string`
- `parse_race_item02`
- `parse_race_table01`
- `parse_premium_lap_summary`
- `parse_race_page_rows`

테스트 방식:

- 작은 HTML 문자열을 테스트 안에 fixture로 둡니다.
- BeautifulSoup 객체를 만들어 parser 함수에 전달합니다.
- 네트워크 요청은 사용하지 않습니다.

필수 케이스:

- `sanitize_text`가 줄바꿈, 탭, 제어문자를 제거하고 문자열이 아닌 값은 그대로 반환하는지 확인합니다.
- `parse_cookie_string`이 쿠키 문자열, dict 입력, 빈 입력을 각각 처리하는지 확인합니다.
- `.RaceList_Item02`가 없을 때 `parse_race_item02`가 기본 dict를 반환하는지 확인합니다.
- 날짜, 요일, 경주명, 출발 시간, 거리, 날씨, 마장 상태, 장소, 출전두수, 상금이 파싱되는지 확인합니다.
- `.RaceTable01`에서 말, 기수, 조교사 ID와 성별/나이, 부담중량이 추출되는지 확인합니다.
- `dusu`가 지정되면 해당 개수까지만 반환하는지 확인합니다.
- `#lap_summary`의 구간 기록이 말 ID 기준으로 row에 병합되는지 확인합니다.

### 2. 출마표 파서

대상: `src/crawlers/WebCrawler/entry_sheet_2/parser.py`

추천 테스트 파일: `tests/test_entry_sheet_parser.py`

테스트 대상:

- `parse_api_entry_sheet_2`

테스트 방식:

- 출마표 HTML 조각과 odds JSON fixture를 함께 사용합니다.
- BeautifulSoup 객체와 `odds_data` dict를 직접 전달합니다.

필수 케이스:

- URL의 `race_id`에서 연도와 경주번호를 추출하는지 확인합니다.
- 날짜, 요일, 경주명, 출발 시간, 거리, 트랙 종류, 방향, 장소, 출전두수가 파싱되는지 확인합니다.
- 말 ID, 기수 ID, 조교사 ID가 링크에서 추출되는지 확인합니다.
- `セ`가 `セン`으로 정규화되는지 확인합니다.
- odds JSON이 있으면 HTML 배당보다 JSON 값을 우선 사용하는지 확인합니다.
- odds JSON이 없으면 HTML 컬럼에서 배당과 인기를 읽는지 확인합니다.
- `.RaceTable01`이 없으면 빈 list를 반환하는지 확인합니다.

### 3. 경주 계획 파서

대상: `src/crawlers/WebCrawler/race_plan/parser.py`

추천 테스트 파일: `tests/test_race_plan_parser.py`

테스트 대상:

- `parse_api_race_plan`
- `parse_pks`

테스트 방식:

- 출마표와 비슷한 HTML 조각을 사용하되, API 필드명 기준으로 기대값을 검증합니다.
- PK 추출은 중복 링크가 있는 HTML을 만들어 set 결과를 확인합니다.

필수 케이스:

- `race_id`에서 `RACE_DT`, `RACE_NO`가 만들어지는지 확인합니다.
- 장소, 날짜, 요일, 경주명, 거리, 트랙, 방향이 파싱되는지 확인합니다.
- 성별 조건 기본값이 `混`인지 확인합니다.
- 상금 필드 `RPM_FPLC`부터 `RPM_FVPLC`까지 파싱되는지 확인합니다.
- `parse_pks`가 HRNO, JKNO, TRNO set을 추출하고 중복을 제거하는지 확인합니다.
- `.RaceTable01`이 없으면 빈 set들을 반환하는지 확인합니다.

### 4. WebCrawler 실행부 유틸

대상: `src/crawlers/WebCrawler/main.py`

추천 테스트 파일: `tests/test_webcrawler_main_utils.py`

테스트 대상:

- `make_race_urls`
- `save_rows_to_csv`

테스트 방식:

- `make_race_urls`는 순수 함수처럼 테스트합니다.
- `save_rows_to_csv`는 가능하면 `tmp_path`와 monkeypatch로 저장 위치를 임시 디렉터리로 바꿔 테스트합니다.

필수 케이스:

- 시작 URL의 마지막 경주번호부터 `max_races`까지 URL을 생성하는지 확인합니다.
- 잘못된 URL이면 `ValueError`가 발생하는지 확인합니다.
- 시작 경주번호가 범위를 벗어나면 1R부터 생성하는지 확인합니다.
- 빈 rows는 파일을 만들지 않고 반환하는지 확인합니다.
- rows가 있으면 header와 row가 utf-8-sig CSV로 저장되는지 확인합니다.

## 2차 단위 테스트 범위

### 5. NAR/noncentral 파서

대상: `src/crawlers/WebCrawler/noncentral.py`

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

2차로 미루는 이유:

- 함수 수가 많고, NAR 전용 HTML 구조를 따로 fixture로 만들어야 합니다.
- `parse_race_header`에 `RCDATE`, `RCDAY` 하드코딩이 있어 테스트 작성 전에 의도 확인이 필요합니다.

### 6. 개최 일정 탐색

대상: `src/crawlers/WebCrawler/discovery.py`

추천 테스트 파일: `tests/test_webcrawler_discovery.py`

테스트 대상:

- `get_upcoming_dates`
- `discover_races`
- `get_all_target_races`

2차로 미루는 이유:

- `datetime.now()`와 `requests.get` 의존이 있습니다.
- HTML 응답과 현재 날짜를 monkeypatch해야 안정적인 테스트가 됩니다.

### 7. PK 분배 CSV 도구

대상: `src/crawlers/WebCrawler/no_divider_from_race_result.py`

추천 테스트 파일: `tests/test_no_divider_from_race_result.py`

테스트 대상:

- `extract_and_save_ids`

2차로 미루는 이유:

- 함수 내부에서 HRNOCrawler, JKNOCrawler, TRNOCrwaler의 `nodata` 디렉터리에 직접 파일을 씁니다.
- 단위 테스트를 안전하게 만들려면 `Path(__file__)` 기준 경로를 monkeypatch하거나 함수 구조를 분리하는 것이 좋습니다.

## 제외 또는 통합 테스트 대상

다음은 1차 단위 테스트에서 제외합니다.

- `src/crawlers/WebCrawler/entry_sheet_2/main.py`: requests, 인증 쿠키, 실제 Netkeiba 응답에 의존합니다.
- `src/crawlers/WebCrawler/main.py`의 Playwright 실행부: 브라우저와 세션 파일이 필요합니다.
- `noncentral.fetch_html`, `noncentral.crawl_one`: 실제 HTTP 요청을 포함합니다.
- `discovery.discover_races`의 실제 requests 호출: mock 없이는 외부 사이트 상태에 따라 결과가 바뀝니다.

## 작성 순서

1. `tests/test_webcrawler_parser.py`
2. `tests/test_entry_sheet_parser.py`
3. `tests/test_race_plan_parser.py`
4. `tests/test_webcrawler_main_utils.py`
5. `tests/test_noncentral_parser.py`
6. `tests/test_webcrawler_discovery.py`
7. `tests/test_no_divider_from_race_result.py`

이 순서로 가면 파서 핵심 로직을 먼저 고정하고, 그 다음 파일 저장과 네트워크 mock이 필요한 부분으로 확장할 수 있습니다.

## 테스트 작성 원칙

- 실제 Netkeiba에 접속하지 않습니다.
- Playwright를 실행하지 않습니다.
- 실제 인증 쿠키나 세션 파일을 요구하지 않습니다.
- 테스트 fixture HTML은 작게 유지합니다.
- `tests/fixtures/webcrawler` 폴더는 필요할 때만 만듭니다.
- 외부 파일 저장이 필요한 테스트는 `tmp_path`를 사용합니다.
- import 시 로그 파일이 `tests` 밖에 쓰이는 모듈은 monkeypatch 또는 import 방식 조정이 필요합니다.

## 예상 리스크

- 일부 모듈은 `from parser import ...`처럼 실행 디렉터리 기준 import를 사용합니다. 테스트에서 파일 경로 기반 import 또는 `sys.path` 조정이 필요할 수 있습니다.
- 일부 파일은 import 시점에 로그 디렉터리와 log file handler를 만듭니다. 테스트 실행 중 원본 프로젝트의 `logs` 폴더에 파일이 생길 수 있습니다.
- `save_rows_to_csv`는 저장 위치를 함수 인자로 받지 않고 모듈 위치 기준 `data` 폴더에 씁니다. 테스트하기 좋게 하려면 저장 경로를 주입받도록 리팩토링하는 것이 좋지만, 현재 역할에서는 원본 코드를 수정하지 않고 monkeypatch로 우회합니다.
- `no_divider_from_race_result.extract_and_save_ids`는 출력 경로가 고정되어 있어 안전한 단위 테스트를 위해 함수 분리가 필요할 수 있습니다.

## 1차 완료 기준

다음 테스트 파일 4개가 작성되고 통과하면 WebCrawler 1차 단위 테스트는 완료로 봅니다.

- `tests/test_webcrawler_parser.py`
- `tests/test_entry_sheet_parser.py`
- `tests/test_race_plan_parser.py`
- `tests/test_webcrawler_main_utils.py`

이 단계에서는 네트워크, 브라우저, 인증, 실제 크롤링 결과까지 보장하지 않습니다. 대신 HTML 구조 변화나 파싱 로직 회귀를 빠르게 잡는 것을 목표로 합니다.
