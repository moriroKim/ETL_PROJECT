import os
from dotenv import load_dotenv
import time
import platform
import sys

# .env 파일 로드
load_dotenv()

# 로컬 데이터베이스 설정
LOCAL_DB_CONFIG = {
    'host': os.getenv('LOCAL_DB_HOST', ''),
    'user': os.getenv('LOCAL_DB_USER', ''),
    'password': os.getenv('LOCAL_DB_PASSWORD', ''),
    'database': os.getenv('LOCAL_DB_NAME', 'etl_project'),
    'port': int(os.getenv('LOCAL_DB_PORT', 3306))
}

# AWS 데이터베이스 설정
AWS_DB_CONFIG = {
    'host': os.getenv('AWS_DB_HOST', 'localhost'),
    'user': os.getenv('AWS_DB_USER', ''),
    'password': os.getenv('AWS_DB_PASSWORD', ''),
    'database': os.getenv('AWS_DB_NAME', 'etl_project'),
    'port': int(os.getenv('AWS_DB_PORT', 3306))
}

# 플랫폼별 UI 문자 설정
if platform.system() == 'Linux':
    LOADING_CHARS = ['|', '/', '-', '\\']
    SUCCESS_CHAR = '√'
    ERROR_CHAR = '×'
    FILE_CHAR = '>'
    COUNT_CHAR = '#'
    START_CHAR = '>'
    END_CHAR = '<'
else:
    LOADING_CHARS = ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏']
    SUCCESS_CHAR = '✓'
    ERROR_CHAR = '✗'
    FILE_CHAR = '→'
    COUNT_CHAR = '•'
    START_CHAR = '→'
    END_CHAR = '←'

def print_loading_effect(message, duration=0.3):
    """로딩 효과를 출력하는 함수"""
    for _ in range(3):  # 3번 반복
        for char in LOADING_CHARS:
            sys.stdout.write(f"\r{char} {message}")
            sys.stdout.flush()
            time.sleep(duration/len(LOADING_CHARS))
    sys.stdout.write('\r' + ' ' * (len(message) + 2) + '\r')
    sys.stdout.flush()

# CSV 파일 경로 설정
def get_csv_files():
    csv_files = {}
    datas_dir = 'datas'
    
    print("\n=== CSV 파일 검색 시작 ===")
    try:
        # datas 폴더 내의 모든 파일 검색
        with os.scandir(datas_dir) as entries:
            for entry in entries:
                # 파일인 경우에만 처리
                if entry.is_file():
                    # 파일 이름 가져오기
                    file_name = entry.name
                    # .csv로 끝나는 파일만 처리
                    if file_name.endswith('.csv'):
                        # 파일 이름에서 .csv 제거
                        table_name = file_name[:-4]
                        # 전체 경로 생성
                        file_path = os.path.join(datas_dir, file_name)
                        # 딕셔너리에 추가
                        csv_files[table_name] = file_path
                        
                        # 로딩 효과 출력
                        print_loading_effect(f"'{table_name}'로딩 중..")
                        print(f"{SUCCESS_CHAR} {table_name} 로드 완료!")
                        print(f"{FILE_CHAR} 파일 경로: {file_path}\n")
                        
    except FileNotFoundError:
        print(f"{ERROR_CHAR} datas 폴더를 찾을 수 없습니다.")
    
    print(f"=== {COUNT_CHAR} 총 {len(csv_files)}개의 CSV 파일을 찾았습니다. ===\n")
    return csv_files

CSV_FILES = get_csv_files()