import requests
from supabase import create_client
import pandas as pd
from io import StringIO
import os
from dotenv import load_dotenv
from typing import Dict, List, Optional

# 환경변수 로드
load_dotenv()

# Supabase 프로젝트 연결 정보
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_API_KEY = os.getenv('API_KEY')
STORAGE_BUCKET = os.getenv('STORAGE_BUCKET')

print(f"Supabase URL: {SUPABASE_URL}")
print(f"Storage Bucket: {STORAGE_BUCKET}")

# Supabase 클라이언트 생성
supabase = create_client(SUPABASE_URL, SUPABASE_API_KEY)

def list_all_files_in_bucket(file_extension: str = None) -> List[str]:
    """
    Supabase Storage API를 사용하여 버킷 내 모든 파일 목록을 가져옵니다.
    
    Args:
        file_extension: 필터링할 파일 확장자 (예: '.csv')
    
    Returns:
        list: 버킷 내 파일 이름 목록
    """
    try:
        # Supabase 클라이언트를 사용하여 버킷의 파일 목록 가져오기
        response = supabase.storage.from_(STORAGE_BUCKET).list(path="datas")
        
        if file_extension:
            # 특정 확장자로 필터링
            files = [file['name'] for file in response if file['name'].endswith(file_extension)]
        else:
            # 모든 파일
            files = [file['name'] for file in response]
            
        print(f"버킷에서 {len(files)}개 파일을 찾았습니다: {files}")
        return files
    
    except Exception as e:
        print(f"파일 목록 가져오기 중 오류 발생: {str(e)}")
        return []

def extract_data_from_supabase(file_names: Optional[List[str]] = None, file_extension: str = '.csv') -> Dict[str, pd.DataFrame]:
    """
    Supabase Storage에서 파일을 가져와 데이터프레임으로 변환합니다.
    
    Args:
        file_names: 가져올 파일명 리스트. None이면 버킷의 모든 파일을 가져옵니다.
        file_extension: 자동으로 가져올 파일 확장자 필터 (기본값: '.csv')
    
    Returns:
        Dict[str, pd.DataFrame]: 추출된 데이터프레임을 담은 딕셔너리
    """
    # 파일명이 지정되지 않은 경우 버킷의 모든 파일 목록 가져오기
    if file_names is None:
        file_names = list_all_files_in_bucket(file_extension)
        
        if not file_names:
            print(f"버킷에서 {file_extension} 형식의 파일을 찾을 수 없습니다.")
            return {}
    
    # 결과를 저장할 딕셔너리
    extracted_data = {}
    
    # 각 파일 처리
    for filename in file_names:
        # 파일 확장자 제거하여 키로 사용
        key = filename.split('.')[0]
        
        try:
            # Supabase 클라이언트를 사용하여 파일 다운로드
            print(f"파일 다운로드 시도: {filename}")
            response = supabase.storage.from_(STORAGE_BUCKET).download(filename)
            
            # 다운로드한 바이너리 데이터를 문자열로 변환
            csv_content = response.decode('utf-8')
            
            # CSV 내용을 DataFrame으로 변환
            df = pd.read_csv(StringIO(csv_content))
            extracted_data[key] = df
            print(f"'{key}' 데이터 추출 완료: {len(df)}행")
            
        except Exception as e:
            print(f"'{key}' 데이터 처리 중 오류 발생: {str(e)}")
    
    return extracted_data

# 테스트: 버킷의 모든 파일 목록 가져오기
list_all_files_in_bucket()

# 테스트: 특정 확장자의 파일만 가져오기
list_all_files_in_bucket('.csv')