from airflow import DAG
from airflow.operators.bash import BashOperator


"""
    The process of starting the root file
"""


default_args = {
        "email": ["santalovdv@mts.ru"],  # santalovdv
        "email_on_failure": True,
        'start_date': "2024-09-10",
        "ssh_conn_id": "oka-analyze-en-001.krd.bd-cloud.mts.ru",
}



with DAG (
    dag_id = "run etl process",
    default_args=default_args,   
   schedule_interval = '0 4 * * 1', # по требованию
    catchup = False) as dag:

    # из вебинтерфейса airflow > admin > connections
    t1 = BashOperator(
    task_id="load",
    bash_command='python /data/airflow/airflow/dags/run_script/main.py')
    
    
t1 
