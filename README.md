# netproj
Dashboard using Data Warehouse


### Software Architecture
![소프트웨어 아키텍처](images/software_architecture.png)
- Python, Pandas, Docker
- Server : Google Compute Engine (ubuntu 22.04.3 LTS, CPU4, RAM16)
- Airflow : v2.6.3
- Superset : v3.0.2
- Data Lake : AWS S3
- Data Warehouse : AWS Redshift
- Data Source :  
[공공데이터포털 : 한국서부발전(주)_신재생에너지 발전량](https://www.data.go.kr/tcs/dss/selectApiDataDetailView.do?publicDataPk=15121592#/API%20%EB%AA%A9%EB%A1%9D/getReGeneration)

### 개선점
1. **Redshift 데이터 적재 시 기본키 보장** :  
   데이터 적재 과정에서 기본키의 일관성과 정확성을 확보하기 위해, 데이터 품질 검증 단계를 추가하거나 적재 전 데이터 전처리 과정을 강화하는 방안이 필요
2. **필요한 데이터만 Redshift에 적재** :  
   S3에 저장된 데이터 중 필요한 부분만 Redshift로 적재하는 전략이 필요. AWS Redshift Spectrum을 활용하여 S3의 데이터를 직접 쿼리하고 필요한 데이터만을 선택적으로 적재할 수 있음
3. **S3 to Redshift 적재 방식의 유연성 개선** :  
   현재의 S3 to Redshift 적재 방식의 유연성 문제를 해결하기 위해, SQL을 직접 작성하여 incremental update 방식으로 적재 프로세스를 개선할 수 있음. 이는 데이터의 변경 사항만을 식별하여 적재하는 방식으로, 효율성과 성능을 개선할 수 있음
4. **Airflow 환경 구축 및 메모리 프로세스 관리 개선** :  
   현재 하나의 서버에 Docker 컨테이너를 사용해서 Airflow 환경을 구축함. 이는 추후 DAG의 수가 많아지면 확장성과 리소스 관리 문제가 발생할 수 있음. 이러한 문제를 해결하기 위해 AWS ECS를 활용한 환경으로의 전환을 고려해볼 수 있음.
5. Open API 데이터 수집 시 스케줄링 전략 필요 :
   Open API에서 데이터를 수집할 때, 정확한 데이터 갱신 시간을 파악하고 이에 맞추어 스케줄을 설정하는 것이 중요하다고 생각함. 단순히 매일 동일한 시간에 데이터를 수집하는 것이 아니라, 데이터의 신규 갱신 시간에 맞춰 최적화된 수집 로직을 구현해야 새로운 데이터를     놓치지 않고 효과적으로 수집할 수 있음
