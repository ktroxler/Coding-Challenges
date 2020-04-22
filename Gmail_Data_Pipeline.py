from airflow.exceptions import AirflowException
from airflow.operators.python_operator import ShortCircuitOperator, PythonOperator
from airflow import models
from airflow import DAG
from operators.gmail_to_csv import gmail_to_csv
from operators.csv_to_postgres import pg_load_table
from airflow.utils.email import send_email
import os
from datetime import datetime, timedelta
from airflow.models import Variable

var_config = Variable.get("Gmail_daily_vars", deserialize_json=True)

def notify_email(contextDict, **kwargs):
    """Send custom email alerts."""

    # email title.
    title = "Airflow ERROR : {dag} Failed".format(**contextDict)

    # email contents
    body = """
    IMPORTANT, <br>
    <br>
    There's been an error in the {dag} job.<br>
    <br>
    Airflow bot <br>
    """.format(**contextDict)

    send_email(var_config['failure_email'], title, body)

def checkforfile():
    if os.listdir(var_config['file_directory']):
        return True
    else:    
        return False

default_dag_args = {
    'owner': var_config['owner'],
    'depends_on_past':var_config['depends_on_past'],
    'start_date': var_config['start_date'],
    'email_on_failure': True,
    'email_on_retry': True,
    'on_failure_callback': notify_email,
    'retries': var_config['retries'],
    'retry_delay': timedelta(minutes=2),
}

with models.DAG(dag_id='Gmail_DAG',schedule_interval = var_config['schedule_interval'],catchup = False,default_args=default_dag_args, on_failure_callback=notify_email) as dag:

    email_to_csv = PythonOperator(task_id='email_to_csv',on_failure_callback=notify_email, python_callable=gmail_to_csv,op_kwargs={'username':var_config['gmail_username'],'password': var_config['gmail_password'],'imap_server': var_config['imap_server'],'inbox_label': var_config['inbox_label'],'csv_file_path': var_config['csv_file_path']})

    checkforfile = ShortCircuitOperator(task_id='checkforfile',provide_context=False,python_callable=checkforfile)

    csv_to_psql = PythonOperator(task_id='csv_to_psql',on_failure_callback=notify_email, python_callable = pg_load_table,op_kwargs={'file_path':var_config['file_path'],'table_name':var_config['table_name'],'dbname':var_config['pg_dbname'],'host':var_config['pg_host'],'port': var_config['pg_port'],'user':var_config['pg_user'],'pwd':var_config['pg_password']})

email_to_csv >> checkforfile >> csv_to_psql