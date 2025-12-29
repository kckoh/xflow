"""
Glue Crawler 관련 헬퍼 함수들
"""
from utils.aws_client import get_aws_client


def check_crawler_status(**kwargs):
    """
    Glue Crawler 상태 확인 (PythonSensor용)

    Returns:
        bool: READY 상태이고 성공하면 True, 아니면 False
    """
    glue = get_aws_client('glue')

    # XCom에서 crawler_name 가져오기 (이전 task에서 저장했다고 가정)
    # 만약 없으면 기본값 사용
    ti = kwargs['ti']
    crawler_name = ti.xcom_pull(
        task_ids='run_glue_crawler',
        key='crawler_name'
    )

    # XCom에 없으면 DAG config에서 가져오기
    if not crawler_name:
        crawler_name = kwargs['dag_run'].conf.get('crawler_name', 'xflow-crawler')

    print(f"Checking crawler status: {crawler_name}")

    try:
        response = glue.get_crawler(Name=crawler_name)
        crawler = response['Crawler']
        state = crawler['State']

        print(f"Crawler state: {state}")

        # READY 상태면 완료된 것
        if state == 'READY':
            last_crawl = crawler.get('LastCrawl', {})
            status = last_crawl.get('Status')

            print(f"Crawler is READY. Last crawl status: {status}")

            if status == 'SUCCEEDED':
                print(f"Crawler completed successfully!")
                return True
            elif status == 'FAILED':
                error_msg = last_crawl.get('ErrorMessage', 'Unknown error')
                raise Exception(f"Crawler failed: {error_msg}")
            else:
                # Status가 없거나 다른 상태면 아직 실행 안 됨
                print(f"Crawler is READY but hasn't run yet")
                return False

        # RUNNING, STOPPING 등 진행 중인 상태
        print(f"Crawler is still running...")
        return False

    except glue.exceptions.EntityNotFoundException:
        raise Exception(f"Crawler not found: {crawler_name}")
    except Exception as e:
        print(f"Error checking crawler status: {e}")
        raise


def get_table_metadata(database_name, table_name):
    """
    Glue Table 메타데이터 가져오기

    Args:
        database_name: Glue 데이터베이스 이름
        table_name: 테이블 이름

    Returns:
        dict: 테이블 메타데이터
    """
    glue = get_aws_client('glue')

    try:
        response = glue.get_table(
            DatabaseName=database_name,
            Name=table_name
        )
        return response['Table']
    except Exception as e:
        print(f"Error fetching table metadata: {e}")
        raise
