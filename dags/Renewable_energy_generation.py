# 모듈 임포트 
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
from airflow.models import XCom
from airflow.exceptions import AirflowException

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
import pendulum

## 로컬 타임존 생성
# local_tz = pendulum.timezone("Asia/Seoul")

BUCKET_NAME = 'net-project'

def create_url(api_key, page_num, start_date, end_date):
    base_url = "http://apis.data.go.kr/B552522/pg/reGeneration/getReGeneration"
    return f"{base_url}?serviceKey={api_key}&pageNo={page_num}&numOfRows=100&startDate={start_date}&endDate={end_date}"


# xml_string을 파싱하여 2차원 리스트로 변환
def parse_xml(xml_string):
    logging.info("Parsing XML")
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
    logging.info("Parsing done")
    return data


# request_count가 몇번에서 실했는지 파악하기 위함
def increment_variable():
    logging.info("Incrementing variable")
    key = "request_count"

    if not Variable.get(key, default_var=None):
        Variable.set(key, "0")
        logging.info("Variable initialized")

    current_value = int(Variable.get(key))
    new_value = current_value + 1
    Variable.set(key, new_value)
    logging.info(f"Variable {key} incremented to {new_value}")


# url을 받아서 요청을 보내고 응답을 반환
def send_request(url):
    max_retries = 5 # 최대 재시도 횟수
    retry_delay = 3 # 초 단위

    for attempt in range(max_retries):  
        try:
            logging.info("Sending request")
            response = requests.get(url, timeout=10)
            response.raise_for_status()  # 상태 코드 검증 (200이 아닌 경우 예외 발생)

        except Exception as e:
            logging.error(f"Attempt {attempt + 1} failed: {e}")
            if attempt < max_retries - 1:  # 마지막 시도가 아니면 재시도 로그 기록
                logging.info(f"Retrying in {retry_delay} seconds...")
            time.sleep(retry_delay)
        else: # 예외가 발생하지 않았다면 루프를 중단하고 응답 결과 반환
            return response.text
        finally: # 요청 실패도 예외로 처리되므로 요청 횟수를 증가시킴
            increment_variable()

    else:  # 모든 재시도가 실패한 경우 (요청 횟수가 초과한 경우가 될 수 있음)
        raise AirflowException(f"Failed to send request after {max_retries} attempts")


# 데이터프레임을 S3에 업로드
def upload_df_to_s3(df, s3_key):
    logging.info("Uploading file to S3")
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
    execution_date = context['ds_nodash']
    start_date = end_date = execution_date

    try:
        initial_url = create_url(api_key, 1, start_date, end_date)
        xml_string = send_request(initial_url)
        
        print(xml_string) # debuging 용도
        soup = BeautifulSoup(xml_string, 'lxml')
        # 각 월별 데이터 개수를 페이지당 100개씩 출력하므로 총 데이터 수에서 100으로 나누어 페이지 수를 계산
        if soup.totalcount is None:
            raise Exception("총 데이터 수를 가져오지 못했습니다.") # totalcount가 없는 경우 dags 자체 재시도
            
        # cnts 기본값 설정
        cnts = int(soup.totalcount.text)
        cnt = math.ceil(cnts / 100)
    
        all_data = []
        for page_num in range(1, cnt + 1):
            url = create_url(api_key, page_num, start_date, end_date)
            xml_string = send_request(url)
            # 데이터가 정상적으로 생성된 경우에만 파싱
            all_data.extend(parse_xml(xml_string))
            # 페이지별로 로그 기록 (나중에 요청이 실패하면 어디까지 진행되었는지 확인하기 위함)
            logging.info(f"Page {page_num}/{cnt} successfully processed.") 
            

        # 데이터 저장 후 로그 메시지 기록
        if all_data:
            df = pd.DataFrame(all_data)
            hour = [f'{i}시' for i in range(1, 25)]
            df.columns = ['날짜', '발전기명'] + hour

            raw_data_s3_key = f'raw_data/extract_data_{execution_date}.csv'
            upload_df_to_s3(df, raw_data_s3_key)

        logging.info("Extract done")
        # 어디에 저장이 됐는지 다음 task에 전달하기 위해 return
        return raw_data_s3_key
        
    except Exception as e:
        logging.exception(f"Error in the extraction process: {e}")
        raise  # 에러를 다시 던져서 에어플로우가 실패로 처리하도록 함




dag = DAG(
    dag_id="net-project-ETL",
    tags=['net-project'],
    start_date=datetime(2024, 1, 23), 
    schedule="@daily",
    catchup=True,
    max_active_runs=1,
    default_args={
        'owner': 'hunsoo',
        'retries': 3,
        'retry_delay': timedelta(minutes=1),
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
