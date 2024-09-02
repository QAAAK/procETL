from airflow import DAG
from airflow.operators.bash import BashOperator

from datetime import date

current_date = date.today()



Class Shedule:
    
    def __init__(self, name_dag:str, update frequency:str, path_dag:str):
        
        self.path = path_dag
        self.frequency = update frequency
        self.start_date = date.today()
        self.name_dag = name_dag
        self.how = None
        
        
        
        

    def __how often():
        
        """     
            consider with what sequence to launch the dag

            :return: void
        
        """
        
        how = None
        
        if lower(self.frequency) == 'daily':
            self.how = '* *'
        elif lower(self.frequency) == 'weekly':
            self.how = '* 1'
        elif lower(self.frequency) == 'monthly':
            self.how = '1 *'
        
            
    def shedule():
        
        """
           The process of starting the root file

            :return: void     
        """
        
        __how_often()
        
        try:
            
            default_args = {
                    "email": ["santalovdv@mts.ru"],  # santalovdv
                    "email_on_failure": True,
                    'start_date': f"{str(self.start_date)}",
                    "ssh_conn_id": "oka-analyze-en-001.krd.bd-cloud.mts.ru",
            }

            with DAG (
                dag_id = self.name_dag,
                default_args=default_args,   
               schedule_interval = f'0 4 * {self.how}', # по требованию
                catchup = False) as dag:

                # из вебинтерфейса airflow > admin > connections
                t1 = BashOperator(
                task_id="load",
                bash_command=f'python {self.path}')


            t1
            
            print('Passed')
            
        except:
            
            print('Error')