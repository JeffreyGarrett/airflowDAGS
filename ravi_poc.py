from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

def print_invoking_spark_job():
    print('Starting spark submit job')


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 4, 28),
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('execute-spark-scala-job',
          default_args=default_args,
          description='Testing Airflow Spark Submit Job',
          schedule_interval='* */3 * * *')

python_operator = PythonOperator(task_id='Invoking_Spark_Job', python_callable=print_invoking_spark_job, dag=dag)

spark_config = {
    'conn_id': 'spark_local',
    'java_class': 'com.spark.airflow.test_spark_airflow',
    'application': '/Users/ravimuthyala/AirflowSparkTestCode/sparkairflowtest_2.12-0.1.jar',
    'jars': '/Users/ravimuthyala/AirflowSparkTestCode/postgresql-42.2.12.jar',
    'application_args': ["/Users/ravimuthyala/AirflowSparkTestCode/receipts.csv"],
    'driver_memory': '1g',
    'executor_cores': 1,
    'num_executors': 1,
    'executor_memory': '1g'
}

spark_submit_operator = SparkSubmitOperator(task_id='Spark_Scala_Submit_Job', dag=dag, **spark_config)

emailNotify = EmailOperator(
   task_id='email_notification',
   to = 'ravi.m@synapsisinc.com',
   subject = 'Spark Submit Job Alert',
   html_content = 'Airflow Spark Submit Job Done',
   dag=dag)

t1Failed = EmailOperator (
    dag=dag,
    trigger_rule=TriggerRule.ONE_FAILED,
    task_id="SparkJobFailed",
    to=["ravi.m@synapsisinc.com"],
    subject="Spark job Failed",
    html_content='<h3>Spark job has failed</h3>')


python_operator.set_downstream(spark_submit_operator)
spark_submit_operator.set_downstream(emailNotify)
t1Failed.set_upstream([spark_submit_operator])

if __name__ == '__main__':
    dag.cli()
