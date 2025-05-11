#!/bin/bash

set -e  # 에러 발생 시 즉시 종료

cd ~/etl_project

# Python 가상환경 생성 및 활성화
python3 -m venv venv
source venv/bin/activate

# 의존성 설치
pip install -r requirements.txt

# .env 파일 생성
cat > .env << EOL
LOCAL_DB_HOST=${LOCAL_DB_HOST}
LOCAL_DB_PORT=${LOCAL_DB_PORT}
LOCAL_DB_USER=${LOCAL_DB_USER}
LOCAL_DB_PASSWORD=${LOCAL_DB_PASSWORD}
LOCAL_DB_NAME=${LOCAL_DB_NAME}

AWS_DB_HOST=${AWS_DB_HOST}
AWS_DB_PORT=${AWS_DB_PORT}
AWS_DB_USER=${AWS_DB_USER}
AWS_DB_PASSWORD=${AWS_DB_PASSWORD}
AWS_DB_NAME=${AWS_DB_NAME}
EOL

# 실행 권한 부여
chmod +x src/*.py

# 크론탭 등록 (중복 제거 후 등록)
(crontab -l 2>/dev/null | grep -v "run_etl.sh"; echo "*/2 * * * * ~/etl_project/run_etl.sh >> ~/etl_project/etl.log 2>&1") | crontab -