from airflow import DAG
from datetime import datetime, timedelta
from daglibs.kafka import DeferrableKafkaSensor

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 12),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'kafka_listener_dag',
    default_args=default_args,
    description='A DAG that consumes and processes Kafka messages with deferrable operator',
    schedule_interval=None,
    catchup=False,
) as dag:

    kafka_sensor_task = DeferrableKafkaSensor(
        task_id='consume_and_process_kafka_messages',
        topic='new_topic',
        bootstrap_servers='kafka:9092',
        group_id='airflow-consumer-group',
        poll_interval=1
    )

    kafka_sensor_task
