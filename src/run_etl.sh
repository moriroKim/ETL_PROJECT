#!/bin/bash

# ETL 프로젝트 디렉토리로 이동
cd /home/ubuntu/ETL_PROJECT

# 가상환경 활성화
source venv/bin/activate

# ETL 스크립트 실행
python3 src/etl_to_aws.py

# 로그 파일 정리 (30일 이상 된 로그 파일 삭제)
find . -name "etl_aws.log.*" -mtime +30 -delete 