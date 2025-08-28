from airflow.decorators import dag, task
from datetime import datetime, timedelta

@dag(
    dag_id="test_dag",
    schedule=None,
    start_date=datetime.now() - timedelta(days=1),
    catchup=False
)
def test():
    @task
    def hello():
        print("hello airflow")
    hello()

test_dag = test()