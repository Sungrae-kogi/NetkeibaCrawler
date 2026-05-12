# 🐎 Netkeiba GIGA Crawler Master Pipeline

## 1. 프로젝트 개요 (Overview)
본 프로젝트는 일본 최대 경마 플랫폼 'Netkeiba(넷케이바)'의 데이터를 수집, 가공, 분석 및 보고하는 **전과정 자동화 데이터 파이프라인**입니다. 단순한 크롤링을 넘어 데이터베이스(MariaDB) 적재, AI 예측 모델 연동, 그리고 최종 결과 리포트 발행까지 하나의 유기적인 시스템으로 통합되어 있습니다.

---

## 2. 주요 핵심 기능 (Core Features)

### 🤖 완전 자동화 오케스트레이션 (`all.py`)
- **스마트 타겟 탐색 (Dynamic Discovery)**: 날짜별로 도쿄, 나카야마, 한신, 교토 등 주요 경기장의 개최 여부를 자동으로 탐지합니다.
- **다중 자동화 모드 (`--auto`)**:
  - `--auto 2/3`: 이번 주 토/일요일 경기 계획(출마표) 자동 수집 및 AI 예측 API 호출.
  - `--auto 4/5`: 지난 토/일요일 경기 결과 수집, DB 업로드 및 **결과 비교 리포트 자동 발송**.
  - `--auto 6`: 지난 주말 경기들의 구간별 기록(Lap Time) 자동 업데이트.
- **실시간 알림**: 작업의 시작, 성공, 실패 여부를 **Telegram Bot**을 통해 실시간으로 전송합니다.

### 📊 데이터 정밀 수집 및 분석
- **전방위 크롤링**: 경기 정보(WebCrawler), 날씨/마장(WeatherCrawler), 상세 프로필(HR/JK/TR Crawler)을 연쇄적으로 수집합니다.
- **날씨 디코딩**: 넷케이바 특유의 난독화된 실시간 예보 데이터를 분석하여 수치화합니다.
- **실시간 변동 추적 (`InformationCrawler`)**: 출주 취소, 기수 변경 등 실시간 공지사항을 즉각적으로 파악합니다.

### 🗄️ 데이터베이스 및 시스템 통합
- **2단계 데이터 이관 전략**: 수집된 CSV를 임시 테이블(`tmp_races`)에 적재한 후, 검증을 거쳐 최종 API용 테이블로 안전하게 이관합니다.
- **AI API 트리거**: 데이터 준비가 완료되면 외부 AI 예측 시스템(`j.mafeel.ai`)에 즉각적으로 API 요청을 보내 분석을 시작합니다.

### 📧 스마트 리포팅 시스템 (`Reporting/email_report.py`)
- **Excel 리포트 생성**: 예측 순위와 실제 결과를 비교하고, 적중 시 하이라이트 처리가 포함된 전문적인 Excel 파일을 자동 생성합니다.
- **SMTP 이메일 배포**: 생성된 리포트를 설정된 수신자(To) 및 참조자(Cc)에게 자동으로 발송합니다.

---

## 3. 시스템 아키텍처 (Architecture)

```mermaid
graph TD
    A[all.py Master] --> B[Discovery Module]
    B --> C{Target Found?}
    C -- Yes --> D[WebCrawler / WeatherCrawler]
    D --> E[HR/JK/TR Detail Crawler]
    E --> F[MariaDB Upsert]
    F --> G[API Transfer]
    G --> H[Trigger External AI API]
    G --> I[Generate Excel Report]
    I --> J[Send Email To/Cc]
    J --> K[Telegram Notification]
```

---

## 4. 운영 가이드 (Operation Guide)

### 🚀 실행 방법
```bash
# 모든 기능이 포함된 마스터 스크립트 실행
python all.py
```

### 🛠️ 주요 모드 설명 (CLI Menu)
1. **과거 결과 수집**: 특정 날짜/장소의 데이터를 소급 수집합니다.
2. **주말 계획 수집**: 이번 주 열릴 경기의 출마표와 날씨를 미리 확보합니다.
6. **DB 업로드/이관**: 수집된 CSV 데이터를 MariaDB로 안전하게 전송합니다.
10. **구간 기록 업데이트**: 경기 종료 후 추가되는 세부 기록(Lap Time)을 보강합니다.

---

## 5. 기술 스택 (Tech Stack)
- **Language**: Python 3.x
- **Library**: Playwright (인증/세션), BeautifulSoup4, Pandas, Openpyxl
- **Database**: MariaDB (PyMySQL)
- **Communication**: SMTP (Email), Telegram Bot API
- **Infrastructure**: Naver Works (발신계정), JRA/Netkeiba Data Source

---

## 6. 문제 해결 및 아키텍처 개선 (Troubleshooting)

### 🚨 실패 사례와 교훈: 복잡한 자가 복구 로직의 폐해
초기에는 네트워크 단절 시 '이어받기(Merge)' 기능을 넣었으나, 파일 구조가 깨지는 버그가 잦았습니다. 이를 해결하기 위해 **'Overwrite-on-run'** 모델로 단순화하고, 대신 **통신 레벨의 강력한 Retry** 로직을 적용하여 시스템의 견고함(Robustness)을 확보했습니다. 현재는 24/7 중단 없이 구동 가능한 수준의 안정성을 보여줍니다.

---

## 7. 향후 로드맵 (Roadmap)
- [ ] **비동기(Async) 처리 확대**: `asyncio` 도입을 통한 크롤링 속도 극대화.
- [ ] **Docker 컨테이너화**: 실행 환경 일관성 유지 및 배포 자동화.
- [ ] **대시보드 구축**: DB 데이터를 시각화하여 실시간 승률 및 적중률 모니터링.

---
*본 문서는 시스템의 지속적인 기능 추가에 따라 주기적으로 업데이트됩니다.*
