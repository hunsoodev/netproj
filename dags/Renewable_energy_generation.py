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


def get_or_initialize_state(**context):
    logging.info("Get or initialize state")
    task_instance = context['ti']
    state = task_instance.xcom_pull(task_ids='update_state', key='http_request_state')
    # 상태 정보가 없는 경우, 초기값으로 설정
    if not state:
        state = {
            'execution_date': context['ds_nodash'],
            'page_num': 1,
            'request_count': 0,
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
def send_request(requests_count, url):
    max_retries = 5  # 최대 재시도 횟수
    retry_delay = 3  # 재시도 간 지연 시간(초)

    for _ in range(max_retries):
        if requests_count > 100:
            logging.info(f"[초과] API 요청 횟수 : {requests_count}")
            return (True, None, requests_count)   

        try:
            response = requests.get(url, timeout=10)  # 타임아웃 설정
            requests_count += 1
            response.raise_for_status()  # 상태 코드 검증
            logging.info(f"API 요청 횟수 : {requests_count}")
            return (True, response.text, requests_count)
        
        except requests.exceptions.HTTPError as e:
            print(f"HTTP 에러: {e}")
        except requests.exceptions.RequestException as e:
            print(f"요청 에러: {e}")
        time.sleep(retry_delay)

    return (False, None, requests_count)


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
    execution_date = state['execution_date']
    default_page_num = state['page_num']
    requests_count = state['request_count']
    start_date = end_date = execution_date

    try:
        initial_url = create_url(api_key, 1, start_date, end_date)
        created, xml_string, requests_count = send_request(requests_count, initial_url)
        if created == False and xml_string is None:
            raise Exception("초기 요청 실패")
        
        print(xml_string) # debuging 용도
        soup = BeautifulSoup(xml_string, 'lxml')
        # 각 월별 데이터 개수를 페이지당 100개씩 출력하므로 총 데이터 수에서 100으로 나누어 페이지 수를 계산
        if soup.totalcount is None:
            raise Exception("총 데이터 수를 가져오지 못했습니다.") # totalcount가 없는 경우 dags 자체 재시도
            
        # cnts 기본값 설정
        cnts = int(soup.totalcount.text)
        cnt = math.ceil(cnts / 100)
    
        all_data = []
        for page_num in range(default_page_num, cnt + 1):
            tmp_page_num = page_num
            url = create_url(api_key, page_num, start_date, end_date)
            created, xml_string, requests_count = send_request(requests_count, url)
            # 데이터가 정상적으로 생성된 경우에만 파싱
            if created == True:
                if xml_string:
                    all_data.extend(parse_xml(xml_string))
                    logging.info(f"Page {page_num}/{cnt} successfully processed.")
                else: # 요청이 초과된 경우 아에 fail 처리(?) or 초과하기 전까지만 데이터 저장(임시방편)
                    logging.info(f"Page {page_num}/{cnt} failed to process.")
                    success = False
                    break
        # 데이터 저장 후 로그 메시지 기록
        if all_data:
            df = pd.DataFrame(all_data)
            hour = [f'{i}시' for i in range(1, 25)]
            df.columns = ['날짜', '발전기명'] + hour

            raw_data_s3_key = f'raw_data/extract_data_{execution_date}.csv'
            upload_df_to_s3(df, raw_data_s3_key)

        logging.info("Extract done")
        return (success, execution_date, tmp_page_num, requests_count)
        
    except Exception as e:
        logging.exception(f"Error in the extraction process: {e}")
        raise  # 에러를 다시 던져서 에어플로우가 실패로 처리하도록 함

# Airflow에서 제공하는 XCom을 사용하여 상태를 저장
def update_state(**kwargs):
    logging.info("Update state")
    task_instance = kwargs['ti']
    result = task_instance.xcom_pull(key='return_value', task_ids='extract')

    if isinstance(result, tuple):  
        success, execu_date, page_num, requests_count = result

    if success == True:
        XCom.clear(
            task_id='get_state',
            dag_id='net-project-ETL',
            execution_date=kwargs['execution_date'])
        logging.info("State deleted")
    else:
        # success == False인 경우 상태를 업데이트
        state = {
            'execution_date': execu_date,
            'page_num': page_num,
            'request_count': requests_count,
            'success': success
        }
        task_instance.xcom_push(key='http_request_state', value=state)
        logging.info(f"State: {state}")




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
