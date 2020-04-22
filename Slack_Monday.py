from datetime import datetime, timedelta
from airflow import DAG
from airflow import models
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from operators.postgres_to_slack import pg_get_data 
from airflow.models import Variable

vars_config = Variable.get('Slack_daily_vars',deserialize_json=True)

default_args = {
    'owner': vars_config['owner'],
    'depends_on_past': vars_config['depends_on_past'],
    'start_date': datetime(2020,4,9),
    'email': vars_config['email'],
    'email_on_failure': False,
    'email_on_retry': True,
    'retries': vars_config['retries'],
    'retry_delay': timedelta(minutes=1),
    'provide_context': True,
    'catchup': False
}

with models.DAG('BBT_daily_challenges_Monday',default_args=default_args,schedule_interval=vars_config['schedule_interval_mon'],catchup=False) as dag:

    tabDays = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
    task_query = []
    task_post = []
    day_num = int(datetime.today().weekday())
    d = datetime.today()
    current_day = d.strftime('%A')

    def get_day(**kwargs):
        kwargs['ti'].xcom_push(key='day',value=datetime.now().weekday())

    def branch(**kwargs):
        return 'postgres_query_for_' + tabDays[kwargs['ti'].xcom_pull(task_ids='get_current_day', key='day')]

    get_weekday = PythonOperator(task_id='get_current_day',python_callable=get_day)

    fork = BranchPythonOperator(task_id='get_current_day_branch',python_callable=branch)

    query =  PythonOperator(task_id='postgres_query_for_' + tabDays[day_num],python_callable=pg_get_data,op_kwargs={'table_name':vars_config['table_name'],'dbname':vars_config['pg_dbname'],'host':vars_config['pg_host'] ,'port': vars_config['pg_port'] ,'user':vars_config['pg_user'] ,'pwd':vars_config['pg_password']})

    post = SlackWebhookOperator(task_id='slack_post_for_' + tabDays[day_num],http_conn_id='slack',webhook_token=vars_config['webhook-token'],
        message= str('{{ task_instance.xcom_pull(task_ids=[postgres_query_for_6])[0] }}'),channel=vars_config['slack_user'],username=vars_config['slack_user'],icon_emoji=':heres_the_t:',link_names=False)
    # key='return_value'
    # provide_context=True
get_weekday >> fork >> query >> post
