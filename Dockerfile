FROM apache/airflow:2.10.2
COPY requirements.txt /requirements.txt
COPY dags/ /opt/airflow/dags/
RUN pip install --no-cache-dir -r /requirements.txt
