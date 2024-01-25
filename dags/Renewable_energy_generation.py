# 모듈 임포트 
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable

from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from datetime import timedelta
from io import StringIO

import pandas as pd
import math
import time
import requests
import logging

BUCKET_NAME = 'net-project'


def create_url(api_key, page_num, start_date, end_date):
    base_url = "http://apis.data.go.kr/B552522/pg/reGeneration/getReGeneration"
    return f"{base_url}?serviceKey={api_key}&pageNo={page_num}&numOfRows=100&startDate={start_date}&endDate={end_date}"

# xml_string을 파싱하여 2차원 리스트로 변환
def parse_xml(xml_string):
    soup = BeautifulSoup(xml_string, 'lxml')
    data = []
    tags = soup.find_all("item")
    for tag in tags:
        tmp = [tag.date.text, tag.gennm.text]
        for i in range(1, 25):
            tag_name = f'q{i:02d}'
            tag_text = getattr(tag, tag_name).text.strip()
            tmp.append(int(tag_text) if tag_text else 0)
        data.append(tmp)
    return data

# url을 받아서 요청을 보내고 응답을 반환
def send_request(url):
    max_retries = 5  # 최대 재시도 횟수
    retry_delay = 5  # 재시도 간 지연 시간(초)

    for _ in range(max_retries):
        try:
            response = requests.get(url, timeout=10)  # 타임아웃 설정
            response.raise_for_status()  # 상태 코드 검증
            return response.text
        except requests.exceptions.HTTPError as e:
            print(f"HTTP 에러: {e}")
        except requests.exceptions.RequestException as e:
            print(f"요청 에러: {e}")
        time.sleep(retry_delay)

    return None


# 데이터프레임을 S3에 업로드
def upload_df_to_s3(df, s3_key):
    try:
        hook = S3Hook(aws_conn_id='netproj_s3_conn_id')

        # 데이터프레임을 문자열 버퍼로 변환
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)

        # S3에 업로드
        hook.load_string(string_data=csv_buffer.getvalue(), bucket_name=BUCKET_NAME, key=s3_key, replace=True)
        logging.info(f"File {s3_key} successfully uploaded to S3 bucket {BUCKET_NAME}")

    except Exception as e:
        logging.error(f"Error occurred while uploading file to S3: {e}")
        raise


def extract(**context):
    logging.info("Extract started")
    api_key = context["params"]["api_key"]
    execution_date = context['ds_nodash']    # Airflow의 execution_date 변수 사용
    
    start_date = end_date = execution_date
    
    try:
        initial_url = create_url(api_key, 1, start_date, end_date)
        
        xml_string = send_request(initial_url)
        if xml_string is None:
            raise Exception("초기 요청 실패")
        
        print(xml_string) # debuging 용도
        soup = BeautifulSoup(xml_string, 'lxml')

        # 각 월별 데이터 개수를 페이지당 100개씩 출력하므로 총 데이터 수에서 100으로 나누어 페이지 수를 계산
        cnts = int(soup.totalcount.text)
        cnt = math.ceil(cnts / 100)
    
        all_data = []
        for page_num in range(1, cnt + 1):
            url = create_url(api_key, page_num, start_date, end_date)
            xml_string = send_request(url)
            if xml_string:
                all_data.extend(parse_xml(xml_string))
                logging.info(f"Page {page_num}/{cnt} successfully processed.")

        # 데이터 저장 후 로그 메시지 기록
        df = pd.DataFrame(all_data)
        hour = [f'{i}시' for i in range(1, 25)]
        df.columns = ['날짜', '발전기명'] + hour

        raw_data_s3_key = f'raw_data/extract_data_{execution_date}.csv'
        upload_df_to_s3(df, raw_data_s3_key)
        
        logging.info("Extract done")
        
    except Exception as e:
        logging.exception(f"Error in the extraction process: {e}")

dag = DAG(
    dag_id="net-project-ETL",
    tags=['net-project'],
    start_date=datetime(2024, 1, 2),
    schedule="@once",
    catchup=False,
    max_active_runs=1,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
    }
)

extract = PythonOperator(
    task_id = 'extract',
    python_callable = extract,
    provide_context = True,
    params = {
        'api_key': Variable.get("api_key"),
    },
    dag = dag
)

extract