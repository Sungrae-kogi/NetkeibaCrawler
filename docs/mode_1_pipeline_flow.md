# 🐎 Mode 1 (과거 경기 결과) 파이프라인 전체 흐름도

과거 경기 결과 데이터가 수집되어 최종 API 테이블까지 안착하는 전체 과정을 한눈에 볼 수 있도록 도식화했습니다.

```mermaid
flowchart TD
    %% 사용자 상호작용
    subgraph User_Input [사용자 입력 단계]
        A([all.py 실행]) --> B{메뉴 1번 선택}
        B --> C[날짜 및 경기장 수동 입력\nex: 20260425, 도쿄]
    end

    %% 크롤링 페이즈
    subgraph Phase_1_Crawling [Phase 1: 데이터 크롤링]
        C --> D(WebCrawler/main.py 가동)
        D -->|결과물| E[race_planning_도쿄_20260425.csv]
        D --> F(하위 크롤러 연쇄 가동)
        
        F --> G(HRNOCrawler)
        F --> H(JKNOCrawler)
        F --> I(TRNOCrawler)
        
        G -->|결과물| J[HRNO_result_*.csv]
        H -->|결과물| K[JKNO_result_*.csv]
        I -->|결나물| L[TRNO_result_*.csv]
    end

    %% 업로드 페이즈 (7번 기능)
    subgraph Phase_2_Upload [Phase 2: DB 업로드 - 7번 메뉴]
        M{메뉴 7번 선택\n+ 타겟 입력} --> N[mariadb_result_upsert.py 실행]
        
        N -->|1. 청소| O[(DB: tmp_races\n해당 경기 DELETE)]
        O -->|2. 데이터 삽입| P[(DB: tmp_races\n결과 CSV INSERT)]
        E -.-> N
        
        N -->|3. 부속 정보 UPSERT| Q[(DB: tmp_horses\ntmp_jockeys\ntmp_trainers)]
        J -.-> N
        K -.-> N
        L -.-> N
    end

    %% 이관 페이즈 (8번 기능)
    subgraph Phase_3_Transfer [Phase 3: API 테이블 이관 - 8번 메뉴]
        R{메뉴 8번 선택\n+ 타겟 입력} --> S[mariadb_result_api_transfer.py 실행]
        
        P -.-> S
        Q -.-> S
        
        S -->|1. JOIN 쿼리 실행| T[(DB: api_race_detail_result_1)]
        S -->|2. JOIN 쿼리 실행| U[(DB: api_race_result)]
    end

    %% 흐름 연결
    E ~~~ M
    Q ~~~ R

    %% 스타일링
    classDef user fill:#f9f,stroke:#333,stroke-width:2px;
    classDef process fill:#bbf,stroke:#333,stroke-width:2px;
    classDef database fill:#fcf,stroke:#333,stroke-width:2px;
    classDef input fill:#cfc,stroke:#333,stroke-width:2px;

    class A,B,M,R user;
    class D,F,G,H,I,N,S process;
    class O,P,Q,T,U database;
    class C,E,J,K,L input;
```

### 💡 주요 핵심 포인트
1. **분리된 크롤링과 적재**: 크롤링(`메뉴 1`)과 적재(`메뉴 7`), 이관(`메뉴 8`)이 완전히 분리되어 있어, DB가 끊기더라도 힘들게 수집한 CSV 데이터는 안전하게 보존됩니다.
2. **선행 청소 (DELETE)**: 7번 기능에서 `tmp_races`에 결과를 밀어넣기 직전, **`DELETE`**를 먼저 쳐서 취소마(Scratched Horse)나 변경된 출전 정보를 깨끗하게 리셋합니다.
3. **무결성 유지 (Rollback)**: 7번, 8번 기능은 내부적으로 트랜잭션 단위로 묶여있어, 중간에 오류가 발생하면 쪼개져서 들어간 데이터들을 싹 없었던 일(Rollback)로 만들고 재시도합니다.
