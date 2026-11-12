from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

JOBS = "/opt/project/jobs"

with DAG(
    dag_id="stackoverflow_monthly",
    description="Kafka -> Bronze -> Silver (Hudi) -> Gold -> modele MLlib",
    start_date=datetime(2024, 1, 1),
    schedule="@monthly",
    catchup=False,
    tags=["bigdata", "IL-06"],
) as dag:
    bronze = BashOperator(task_id="bronze_ingest", bash_command=f"cd {JOBS} && python bronze.py")
    silver = BashOperator(task_id="silver_hudi", bash_command=f"cd {JOBS} && python silver.py")
    gold = BashOperator(task_id="gold_trends", bash_command=f"cd {JOBS} && python gold.py")
    train = BashOperator(task_id="train_model", bash_command=f"cd {JOBS} && python train_model.py")

    bronze >> silver >> gold >> train
