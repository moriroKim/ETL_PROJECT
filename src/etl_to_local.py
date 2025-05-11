import pandas as pd
from pymysql import connect
from sqlalchemy import create_engine
from config import LOCAL_DB_CONFIG, CSV_FILES, print_loading_effect
from config import (
    SUCCESS_CHAR, ERROR_CHAR, START_CHAR, END_CHAR, print_loading_effect
)
from urllib.parse import quote_plus
from datetime import datetime

# 비밀번호 URL 인코딩
encoded_password = quote_plus(LOCAL_DB_CONFIG['password'])

# SQLAlchemy 엔진 생성
connection_string = f"mysql+pymysql://{LOCAL_DB_CONFIG['user']}:{encoded_password}@{LOCAL_DB_CONFIG['host']}:{LOCAL_DB_CONFIG['port']}/{LOCAL_DB_CONFIG['database']}"
engine = create_engine(
    connection_string
)

def create_database():
    print("\n=== 데이터베이스 생성 시작 ===")
    try:
        # MySQL 연결
        print_loading_effect("MySQL 서버에 연결 중...")
        conn = connect(
            host=LOCAL_DB_CONFIG['host'],
            user=LOCAL_DB_CONFIG['user'],
            password=LOCAL_DB_CONFIG['password'],
            port=LOCAL_DB_CONFIG['port']
        )
        cursor = conn.cursor()
        
        # 데이터베이스 생성
        print_loading_effect(f"데이터베이스 '{LOCAL_DB_CONFIG['database']}' 생성 중...")
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {LOCAL_DB_CONFIG['database']}")
        cursor.close()
        conn.close()
        print(f"{SUCCESS_CHAR} 데이터베이스 '{LOCAL_DB_CONFIG['database']}' 생성 완료!")
    except Exception as e:
        print(f"{ERROR_CHAR} 데이터베이스 생성 중 오류 발생: {str(e)}")
        raise

def import_csv_to_mysql():
    print("\n=== CSV 데이터 이관 시작 ===")
    for table_name, csv_file in CSV_FILES.items():
        try:
            # CSV 파일 읽기
            print_loading_effect(f"{table_name} CSV 파일 읽는 중...")
            df = pd.read_csv(csv_file)
            
            # 타임스탬프 컬럼 추가
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            df['etl_timestamp'] = current_time
            
            # 데이터프레임을 MySQL 테이블로 저장
            print_loading_effect(f"{table_name} 테이블에 데이터 저장 중...")
            df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)
            print(f"{SUCCESS_CHAR} {table_name} 테이블 생성 및 데이터 이관 완료!")
            print(f"[+] 총 {len(df)}개의 레코드가 이관되었습니다.")
            print(f"[+] 이관 시간: {current_time}\n")
        except Exception as e:
            print(f"{ERROR_CHAR} {table_name} 테이블 처리 중 오류 발생: {str(e)}")

if __name__ == "__main__":
    try:
        print(f"\n{START_CHAR} ETL 프로세스 시작")
        print("=" * 50)
        create_database()
        import_csv_to_mysql()
        print(f"\n{END_CHAR} 모든 데이터 이관이 완료되었습니다!")
        print("=" * 50)
    except Exception as e:
        print(f"\n{ERROR_CHAR} 오류 발생: {str(e)}")
        print("=" * 50) 