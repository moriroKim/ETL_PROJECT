# ETL 프로젝트

이 프로젝트는 데이터를 추출(Extract), 변환(Transform), 적재(Load)하는 ETL 파이프라인을 구현합니다.

## 디렉토리 구조

```
etl_project/
├── app/                           # FastAPI 백엔드 서버
│   ├── api/                       # API 라우팅
│   │   ├── endpoints/
│   │   │   └── data.py            # 예: /data 엔드포인트
│   │   └── dependencies.py
│   ├── core/                      # 설정 관련 (config, DB 연결 등)
│   │   ├── config.py
│   │   └── db.py
│   ├── models/                    # Pydantic & ORM 모델
│   │   └── item.py
│   └── main.py                    # FastAPI 실행 진입점
│
├── etl/                           # ETL 로직 (API → DB)
│   ├── extract.py                 # 데이터 추출
│   ├── transform.py               # 데이터 변환
│   ├── load.py                    # 데이터 적재
│   └── runner.py                  # ETL 전체 실행 스크립트
│
├── config/                        # 설정 파일
│   └── instance.py                # 환경변수 등 설정 관리
│
├── data/                          # 데이터 파일 (CSV 등)
│
├── logs/                          # 로그 파일
│   └── etl.log
│
├── scripts/                       # 실행 스크립트
│   └── run_etl.sh
│
├── requirements.txt               # 의존성 패키지
├── .env                           # 환경변수 파일
└── README.md                      # 프로젝트 설명
```

## 설치 및 실행

### 1. 가상환경 설정

```bash
# 가상환경 생성
python -m venv .etlcore

# 가상환경 활성화 (Windows)
.etlcore\Scripts\activate

# 가상환경 활성화 (Linux/Mac)
source .etlcore/bin/activate
```

### 2. 의존성 패키지 설치

```bash
pip install -r requirements.txt
```

### 3. 환경변수 설정

`.env` 파일을 생성하고 다음과 같이 설정합니다:

```
DB_URL=mysql+pymysql://username:password@localhost:3306/database_name
API_URL=http://localhost:8000/api
API_KEY=your_api_key_here
```

### 4. ETL 실행

```bash
# 직접 실행
python etl/runner.py

# 또는 스크립트 사용
bash scripts/run_etl.sh
```

## 주요 기능

### 데이터 추출 (Extract)

`etl/extract.py`에서 CSV 파일에서 데이터를 추출합니다.

### 데이터 변환 (Transform)

`etl/transform.py`에서 추출된 데이터를 변환하고 정제합니다.

### 데이터 적재 (Load)

`etl/load.py`에서 변환된 데이터를 데이터베이스에 저장합니다.
