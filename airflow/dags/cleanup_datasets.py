from garbage_cleanup.garbage_cleanup import cleanup_data
from garbage_cleanup.fuzzy_cleanup import fuzzy_group_data
from datetime import datetime, timedelta
from airflow.decorators import dag, task
    
@task(pool='duckdb_pool')
def cleanup_tenant_data():
    cleanup_data(
        schema='main_silver',
        table_name='latest_tenant_data'
    )
    
@task(pool='duckdb_pool')
def cleanup_hist_tenant_data():
    cleanup_data(
        schema='main_silver',
        table_name='hist_tenant_data'
    )
    
@task(pool='duckdb_pool')
def fuzzy_cleanup_tenant_data():
    fuzzy_group_data(
        schema='main_silver',
        table_name='latest_tenant_data',
        column='ReferralAgency', 
        group_column_name='GroupedReferralAgency'
    )
    
@task(pool='duckdb_pool')
def fuzzy_cleanup_hist_tenant_data():
    fuzzy_group_data(
        schema='main_silver',
        table_name='hist_tenant_data',
        column='ReferralAgency', 
        group_column_name='GroupedReferralAgency'
    )
    
@task.bash(pool='duckdb_pool')
def task_close_duckdb_connection():
    return "/home/asha/airflow_env/bin/python -c \"import duckdb; conn = duckdb.connect('/home/asha/data_lake/asha_prod.duckdb'); conn.execute('CHECKPOINT'); conn.close(); print('DuckDB checkpoint complete and connection closed')\""
       
@dag(
    dag_id="cleanup_datasets",
    schedule=None,
    start_date=datetime.now() - timedelta(days=1),
    catchup=False,
    tags=["optimisation"]
)
def garbage_cleanup():
    t1 = cleanup_tenant_data()
    t2 = task_close_duckdb_connection.override(task_id='close_duckdb_after_cleanup')() 
    t3 = fuzzy_cleanup_tenant_data()
    t4 = task_close_duckdb_connection.override(task_id='close_duckdb_after_fuzzy_cleanup')()
    t5 = cleanup_hist_tenant_data()
    t6 = task_close_duckdb_connection.override(task_id='close_duckdb_after_hist_cleanup')()
    t7 = fuzzy_cleanup_hist_tenant_data()
    t8 = task_close_duckdb_connection.override(task_id='close_duckdb_after_fuzzy_hist_cleanup')()
    
    t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7 >> t8
   
dag_instance = garbage_cleanup()