import pandas as pd
from pymysql import connect
from sqlalchemy import create_engine, text
from config import AWS_DB_CONFIG, LOCAL_DB_CONFIG, print_loading_effect
from urllib.parse import quote_plus
from datetime import datetime
import logging
import os
from dotenv import load_dotenv
from config import (
    SUCCESS_CHAR, ERROR_CHAR, print_loading_effect
)

# 로깅 설정
logging.basicConfig(
    filename='etl_aws.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

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

def create_aws_database():
    """AWS EC2에 데이터베이스 생성"""
    print("\n=== AWS 데이터베이스 생성 시작 ===")
    try:
        # AWS MySQL 연결
        print_loading_effect("AWS MySQL 서버에 연결 중...")
        conn = connect(
            host=AWS_DB_CONFIG['host'],
            user=AWS_DB_CONFIG['user'],
            password=AWS_DB_CONFIG['password'],
            port=AWS_DB_CONFIG['port']
        )
        cursor = conn.cursor()
        
        # 데이터베이스 생성
        print_loading_effect(f"AWS 데이터베이스 '{AWS_DB_CONFIG['database']}' 생성 중...")
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {AWS_DB_CONFIG['database']}")
        cursor.close()
        conn.close()
        print(f"{SUCCESS_CHAR} AWS 데이터베이스 '{AWS_DB_CONFIG['database']}' 생성 완료!")
    except Exception as e:
        print(f"{ERROR_CHAR} AWS 데이터베이스 생성 중 오류 발생: {str(e)}")
        raise

def get_local_tables():
    """로컬 데이터베이스의 테이블 목록 조회"""
    try:
        # 로컬 MySQL 연결
        conn = connect(
            host=LOCAL_DB_CONFIG['host'],
            user=LOCAL_DB_CONFIG['user'],
            password=LOCAL_DB_CONFIG['password'],
            database=LOCAL_DB_CONFIG['database'],
            port=LOCAL_DB_CONFIG['port']
        )
        cursor = conn.cursor()
        
        # 테이블 목록 조회
        cursor.execute("SHOW TABLES")
        tables = [table[0] for table in cursor.fetchall()]
        
        cursor.close()
        conn.close()
        return tables
    except Exception as e:
        error_msg = f"로컬 테이블 목록 조회 중 오류 발생: {str(e)}"
        logging.error(error_msg)
        raise Exception(error_msg)

def create_parameter_table(engine):
    """파라미터 테이블 생성"""
    try:
        query = """
        CREATE TABLE IF NOT EXISTS etl_parameters (
            param_id INT AUTO_INCREMENT PRIMARY KEY,
            param_name VARCHAR(50) NOT NULL,
            param_value TEXT,
            description TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        )
        """
        with engine.connect() as conn:
            conn.execute(text(query))
        
        # 기본 파라미터 값 설정
        default_params = [
            ('last_etl_date', None, '마지막 ETL 실행 일시'),
            ('batch_size', '1000', '한 번에 처리할 레코드 수'),
            ('retention_days', '30', '데이터 보관 기간(일)'),
            ('error_threshold', '100', '오류 허용 임계값')
        ]
        
        for param in default_params:
            query = """
            INSERT IGNORE INTO etl_parameters (param_name, param_value, description)
            VALUES (:param_name, :param_value, :description)
            """
            with engine.connect() as conn:
                conn.execute(text(query), {
                    'param_name': param[0],
                    'param_value': param[1],
                    'description': param[2]
                })
                
    except Exception as e:
        error_msg = f"파라미터 테이블 생성 중 오류 발생: {str(e)}"
        logging.error(error_msg)
        raise Exception(error_msg)

def get_parameter(engine, param_name):
    """파라미터 값 조회"""
    try:
        query = "SELECT param_value FROM etl_parameters WHERE param_name = :param_name"
        with engine.connect() as conn:
            result = conn.execute(text(query), {'param_name': param_name}).fetchone()
            if result is None:
                # 파라미터가 없으면 기본값 반환
                default_values = {
                    'batch_size': '1000',
                    'retention_days': '30',
                    'error_threshold': '100',
                    'last_etl_date': None
                }
                return default_values.get(param_name)
            return result[0]
    except Exception as e:
        error_msg = f"파라미터 조회 중 오류 발생: {str(e)}"
        logging.error(error_msg)
        raise Exception(error_msg)

def update_parameter(engine, param_name, param_value):
    """파라미터 값 업데이트"""
    try:
        query = "UPDATE etl_parameters SET param_value = :param_value WHERE param_name = :param_name"
        with engine.connect() as conn:
            conn.execute(text(query), {
                'param_value': param_value,
                'param_name': param_name
            })
    except Exception as e:
        error_msg = f"파라미터 업데이트 중 오류 발생: {str(e)}"
        logging.error(error_msg)
        raise Exception(error_msg)

def validate_row_count(source_df, target_df, table_name):
    """건수 체크 (Row Count Validation)"""
    source_count = len(source_df)
    target_count = len(target_df)
    
    if source_count != target_count:
        error_msg = f"{table_name} 테이블 건수 불일치: 소스={source_count}, 타겟={target_count}"
        logging.error(error_msg)
        raise Exception(error_msg)
    
    logging.info(f"{table_name} 테이블 건수 일치: {source_count}건")

def validate_aggregation(source_df, target_df, group_cols, sum_cols, table_name):
    """GROUP BY SUM 확인 (Aggregation Validation)"""
    for col in sum_cols:
        source_sum = source_df[col].sum()
        target_sum = target_df[col].sum()
        
        if abs(source_sum - target_sum) > 0.01:  # 부동소수점 오차 고려
            error_msg = f"{table_name} 테이블 {col} 합계 불일치: 소스={source_sum}, 타겟={target_sum}"
            logging.error(error_msg)
            raise Exception(error_msg)
        
        logging.info(f"{table_name} 테이블 {col} 합계 일치: {source_sum:,.2f}")

def check_duplicates(df, key_cols, table_name):
    """데이터 중복 확인 (Duplicate Check)"""
    duplicates = df.duplicated(subset=key_cols, keep=False)
    if duplicates.any():
        duplicate_count = duplicates.sum()
        error_msg = f"{table_name} 테이블 중복 데이터 발견: {duplicate_count}건"
        logging.error(error_msg)
        raise Exception(error_msg)
    
    logging.info(f"{table_name} 테이블 중복 데이터 없음")

def check_null_values(df, required_cols, table_name):
    """NULL 값 확인 (Null Check)"""
    null_counts = df[required_cols].isnull().sum()
    null_cols = null_counts[null_counts > 0]
    
    if not null_cols.empty:
        error_msg = f"{table_name} 테이블 NULL 값 발견:\n"
        for col, count in null_cols.items():
            error_msg += f"- {col}: {count}건\n"
        logging.error(error_msg)
        raise Exception(error_msg)
    
    logging.info(f"{table_name} 테이블 필수 컬럼 NULL 값 없음")

def validate_data_range(df, range_checks, table_name):
    """데이터 범위 검증 (Range Check)"""
    for col, (min_val, max_val) in range_checks.items():
        out_of_range = df[(df[col] < min_val) | (df[col] > max_val)]
        if not out_of_range.empty:
            error_msg = f"{table_name} 테이블 {col} 범위 초과 데이터 발견: {len(out_of_range)}건"
            logging.error(error_msg)
            raise Exception(error_msg)
        
        logging.info(f"{table_name} 테이블 {col} 범위 검증 통과: {min_val} ~ {max_val}")

def validate_report_data(source_df, target_df):
    """보고 데이터 검증"""
    table_name = 'customer_loan_transaction_report'
    
    # 1. 건수 체크
    validate_row_count(source_df, target_df, table_name)
    
    # 2. 금액 합계 검증
    sum_cols = ['total_loan_amount', 'total_transaction_amount']
    validate_aggregation(source_df, target_df, ['customer_id'], sum_cols, table_name)
    
    # 3. 중복 데이터 확인
    key_cols = ['customer_id', 'report_generated_at']
    check_duplicates(target_df, key_cols, table_name)
    
    # 4. NULL 값 확인
    required_cols = ['customer_id', 'name', 'total_loans', 'total_loan_amount']
    check_null_values(target_df, required_cols, table_name)
    
    # 5. 데이터 범위 검증
    range_checks = {
        'total_loan_amount': (0, 1000000000),  # 0 ~ 10억
        'total_transaction_amount': (0, 1000000000),  # 0 ~ 10억
        'total_loans': (0, 1000),  # 0 ~ 1000건
        'total_transactions': (0, 10000)  # 0 ~ 10000건
    }
    validate_data_range(target_df, range_checks, table_name)

def generate_report_data(local_engine, batch_size=1000):
    """로컬 DB의 데이터를 기반으로 보고 데이터 생성"""
    try:
        # 예시: 고객별 대출 및 거래 통계 보고서
        query = """
        SELECT 
            c.customer_id,
            c.name,
            c.birth_date,
            c.gender,
            COUNT(DISTINCT l.loan_id) as total_loans,
            SUM(l.loan_amount) as total_loan_amount,
            COUNT(DISTINCT t.transaction_id) as total_transactions,
            SUM(t.amount) as total_transaction_amount,
            MAX(t.transaction_date) as last_transaction_date
        FROM customer_info c
        LEFT JOIN loan_info l ON c.customer_id = l.customer_id
        LEFT JOIN transaction_info t ON c.customer_id = t.account_id
        GROUP BY c.customer_id, c.name, c.birth_date, c.gender
        """
        
        # 보고 데이터 생성
        report_df = pd.read_sql_query(query, local_engine)
        
        # 추가 통계 계산
        report_df['avg_loan_amount'] = report_df['total_loan_amount'] / report_df['total_loans'].replace(0, 1)
        report_df['avg_transaction_amount'] = report_df['total_transaction_amount'] / report_df['total_transactions'].replace(0, 1)
        
        # 보고 생성 시간 추가
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        report_df['report_generated_at'] = current_time
        
        return report_df, current_time
        
    except Exception as e:
        error_msg = f"보고 데이터 생성 중 오류 발생: {str(e)}"
        logging.error(error_msg)
        raise Exception(error_msg)

def transfer_to_aws():
    """로컬 데이터를 AWS로 이관"""
    start_time = datetime.now()
    logging.info("=== AWS 데이터 이관 시작 ===")
    
    try:
        # AWS 데이터베이스 생성
        create_aws_database()
        
        # 로컬 연결 문자열 생성
        local_encoded_password = quote_plus(LOCAL_DB_CONFIG['password'])
        local_connection_string = f"mysql+pymysql://{LOCAL_DB_CONFIG['user']}:{local_encoded_password}@{LOCAL_DB_CONFIG['host']}:{LOCAL_DB_CONFIG['port']}/{LOCAL_DB_CONFIG['database']}"
        local_engine = create_engine(local_connection_string)
        
        # AWS 연결 문자열 생성
        aws_encoded_password = quote_plus(AWS_DB_CONFIG['password'])
        aws_connection_string = f"mysql+pymysql://{AWS_DB_CONFIG['user']}:{aws_encoded_password}@{AWS_DB_CONFIG['host']}:{AWS_DB_CONFIG['port']}/{AWS_DB_CONFIG['database']}"
        aws_engine = create_engine(aws_connection_string)
        
        # 파라미터 테이블 생성 및 기본값 설정
        create_parameter_table(aws_engine)
        
        # 배치 사이즈 가져오기 (기본값 1000)
        batch_size = int(get_parameter(aws_engine, 'batch_size') or 1000)
        
        # 보고 데이터 생성
        logging.info("보고 데이터 생성 중...")
        source_df, current_time = generate_report_data(local_engine, batch_size)
        
        # AWS로 데이터 이관 (누적 저장)
        logging.info("보고 데이터를 AWS로 이관 중...")
        source_df.to_sql(
            name='customer_loan_transaction_report',
            con=aws_engine,
            if_exists='append',  # 누적 저장을 위해 'append' 사용
            index=False
        )
        
        # 이관된 데이터 검증
        logging.info("데이터 검증 중...")
        target_df = pd.read_sql_table('customer_loan_transaction_report', aws_engine)
        validate_report_data(source_df, target_df)
        
        # 마지막 ETL 실행 시간 업데이트
        update_parameter(aws_engine, 'last_etl_date', current_time)
        
        # 이관 결과 로깅
        end_time = datetime.now()
        duration = end_time - start_time
        logging.info(f"=== AWS 데이터 이관 완료 ===")
        logging.info(f"총 {len(source_df)}개의 보고 레코드 이관")
        logging.info(f"소요 시간: {duration}")
        
        # 보고 요약 정보 로깅
        logging.info("\n=== 보고 요약 ===")
        logging.info(f"총 고객 수: {len(source_df)}")
        logging.info(f"총 대출 건수: {source_df['total_loans'].sum()}")
        logging.info(f"총 대출 금액: {source_df['total_loan_amount'].sum():,.0f}")
        logging.info(f"총 거래 건수: {source_df['total_transactions'].sum()}")
        logging.info(f"총 거래 금액: {source_df['total_transaction_amount'].sum():,.0f}")
        
    except Exception as e:
        error_msg = f"전체 이관 프로세스 중 오류 발생: {str(e)}"
        logging.error(error_msg)
        raise Exception(error_msg)

if __name__ == "__main__":
    try:
        transfer_to_aws()
    except Exception as e:
        logging.error(f"프로그램 실행 중 오류 발생: {str(e)}")
        raise 