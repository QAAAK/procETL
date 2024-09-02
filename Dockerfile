FROM python:3.9


RUN apk update && apk upgrade && apk add bash


COPY . .


RUN ["/home/airflow/airflow/dags/", "/procETL"]

CMD ["bash", "pip install -r requirements.txt"]
CMD ["python", "./name_dag.py"]

