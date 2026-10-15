from datetime import timedelta

import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="realtime_fraud_gold_batch",
    description="Build Gold Delta marts from Silver CDC events",
    start_date=pendulum.datetime(
        2026, 10, 1, tz="Africa/Cairo"
    ),
    schedule="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-engineering",
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["fraud", "spark", "delta", "gold"],
) as dag:

    build_gold_marts = BashOperator(
        task_id="build_gold_marts",
        bash_command=(
            "set -euo pipefail && "
            "docker exec spark-silver "
            "/opt/spark/bin/spark-submit "
            "--driver-memory 512m "
            "/opt/jobs/gold_batch.py"
        ),
        execution_timeout=timedelta(hours=1),
        do_xcom_push=False,
    )