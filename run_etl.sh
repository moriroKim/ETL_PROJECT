#!/bin/bash

PROJECT_DIR="/home/ubuntu/etl_project"
VENV_DIR="$PROJECT_DIR/venv"
LOG_DIR="$PROJECT_DIR/logs"
LOG_FILE="$LOG_DIR/etl_aws.log.$(date +\%Y-\%m-\%d_\%H:\%M)"

mkdir -p "$LOG_DIR"
source "$VENV_DIR/bin/activate"
pip install -r "$PROJECT_DIR/requirements.txt"
python3 "$PROJECT_DIR/src/etl_to_aws.py" >> "$LOG_FILE" 2>&1
find "$LOG_DIR" -name "etl_aws.log.*" -mtime +30 -delete