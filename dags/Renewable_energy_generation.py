# 모듈 임포트 
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
from airflow.models import XCom

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
REQUEST_COUNT = 0


def get_or_initialize_state(**kwargs):
    logging.info("Get or initialize state")
    global REQUEST_COUNT
    task_instance = kwargs['ti']
    state = task_instance.xcom_pull(task_ids='update_state', key='http_request_state')
    # 상태 정보가 없는 경우, 초기값으로 설정
    if not state:
        state = {
            'last_execution_date': kwargs['ds_nodash'],
            'last_page': 1,
            'request_count': REQUEST_COUNT,
            'success': None
        }
        # 초기 상태를 XCom에 저장
        task_instance.xcom_push(key='http_request_state', value=state)
    logging.info(f"State: {state}")


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


# url을 받아서 요청을 보내고 응답을 반환
def send_request(url):
    global REQUEST_COUNT
    max_retries = 5  # 최대 재시도 횟수
    retry_delay = 5  # 재시도 간 지연 시간(초)

    for _ in range(max_retries):
        if REQUEST_COUNT > 100:
            logging.info(f"[초과] API 요청 횟수 : {REQUEST_COUNT}")
            return (True, None)   
        
        try:
            response = requests.get(url, timeout=10)  # 타임아웃 설정
            REQUEST_COUNT += 1
            response.raise_for_status()  # 상태 코드 검증
            logging.info(f"API 요청 횟수 : {REQUEST_COUNT}")
            return (True, response.text)
        except requests.exceptions.HTTPError as e:
            print(f"HTTP 에러: {e}")
        except requests.exceptions.RequestException as e:
            print(f"요청 에러: {e}")
        time.sleep(retry_delay)

    return (False, None)


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
    success = True
    api_key = context["params"]["api_key"]
    task_instance = context['ti']
    state = task_instance.xcom_pull(task_ids='get_state', key='http_request_state')
    execution_date = state['last_execution_date']
    default_page_num = state['last_page']
    start_date = end_date = execution_date

    try:
        initial_url = create_url(api_key, 1, start_date, end_date)
        flag, xml_string = send_request(initial_url)
        if flag == False and xml_string is None:
            raise Exception("초기 요청 실패")
        # print(xml_string) # debuging 용도
        soup = BeautifulSoup(xml_string, 'lxml')
        # 각 월별 데이터 개수를 페이지당 100개씩 출력하므로 총 데이터 수에서 100으로 나누어 페이지 수를 계산
        if soup.totalcount is None:
            logging.info("총 데이터 수를 가져오지 못했습니다.")
        # cnts 기본값 설정
        cnts = int(soup.totalcount.text) if soup.totalcount.text else 0
        cnt = math.ceil(cnts / 100)
    
        all_data = []
        for page_num in range(default_page_num, cnt + 1):
            url = create_url(api_key, page_num, start_date, end_date)
            flag, xml_string = send_request(url)
            if flag == True:
                if xml_string is not None:
                    all_data.extend(parse_xml(xml_string))
                    logging.info(f"Page {page_num}/{cnt} successfully processed.")
                else:
                    success = False
                    logging.info(f"Page {page_num}/{cnt} failed to process.")
                    return (success, execution_date, page_num)
        
        if all_data:
            # 데이터 저장 후 로그 메시지 기록
            df = pd.DataFrame(all_data)
            hour = [f'{i}시' for i in range(1, 25)]
            df.columns = ['날짜', '발전기명'] + hour

            raw_data_s3_key = f'raw_data/extract_data_{execution_date}.csv'
            upload_df_to_s3(df, raw_data_s3_key)

        logging.info("Extract done")
        return success
        
    except Exception as e:
        logging.exception(f"Error in the extraction process: {e}")


# Airflow에서 제공하는 XCom을 사용하여 상태를 저장
def update_state(**kwargs):
    global REQUEST_COUNT
    logging.info("Update state")
    task_instance = kwargs['ti']
    result = task_instance.xcom_pull(key='return_value', task_ids='extract')

    if isinstance(result, tuple):  
        success, execution_date, page_num = result
    else:
        success = result

    if success == True:
        XCom.clear(
            task_ids=['get_state', 'extract', 'update_state'],
            dag_id='net-project-ETL', 
            execution_date=kwargs['execution_date'])
        logging.info("State deleted")
    else:
        # success == False인 경우 상태를 업데이트
        state = {
            'last_execution_date': execution_date,
            'last_page': page_num,
            'request_count': REQUEST_COUNT,
            'success': success
        }
        task_instance.xcom_push(key='http_request_state', value=state)
        logging.info(f"State: {state}")




dag = DAG(
    dag_id="net-project-ETL",
    tags=['net-project'],
    start_date=datetime(2024, 1, 17), 
    schedule="@daily",
    catchup=True,
    max_active_runs=2,
    default_args={
        'owner': 'hunsoo',
        'retries': 1,
        'retry_delay': timedelta(minutes=2),
    }
)


get_state_task = PythonOperator(
    task_id='get_state',
    python_callable=get_or_initialize_state,
    provide_context=True,
    dag=dag
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


update_state_task = PythonOperator(
    task_id = 'update_state',
    python_callable = update_state,
    provide_context = True,
    dag = dag
)

get_state_task >> extract >> update_state_task