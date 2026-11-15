import asyncio
import time
import asyncpg
import sys
import os

import pendulum

sys.path.insert(0, os.path.expanduser("~/PycharmProjects/etl"))
from airflow.operators.email import EmailOperator
from validation import DataValidator
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from datetime import timedelta
from env_config import db_settings

from airflow.sensors.filesystem import FileSensor

from etl_process import main, ETLConfig

from config import BASE_DIR

args_default = {
    "owner": "etl_bank",
    "start_date": pendulum.datetime(2024, 1, 1, tz="UTC"),
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "sla": timedelta(minutes=30),
}


async def get_db_conn():
    return await asyncpg.connect(
        host=db_settings.db_host,
        port=db_settings.db_port,
        user=db_settings.db_user,
        password=db_settings.db_password,
        database=db_settings.db_database,
    )


def load(**context):
    try:
        start = time.perf_counter()
        count_records = asyncio.run(main())
        duration = time.perf_counter() - start

        context["ti"].xcom_push(key="records", value=count_records)
        context["ti"].xcom_push(key="duration", value=duration)
        context["ti"].xcom_push(key="result", value="success")

    except asyncpg.ConnectionFailureError:
        context["ti"].xcom_push(key="result", value="db_error")
        raise

    except FileNotFoundError:
        context["ti"].xcom_push(key="result", value="files_missing")
        raise

    except Exception as e:
        context["ti"].xcom_push(key="result", value="unknown_error")
        raise


def validation(**context):
    context["ti"].xcom_pull(task_ids="load", key="records")

    async def valid():
        conn = await get_db_conn()
        try:
            etl_config = ETLConfig()
            validator = DataValidator(etl_config.stop_on_validation_error)
            error_count = await validator.validate_all(conn)
            result = {
                "has_warning": True if error_count > 0 else False,
                "error_count": error_count,
                "validation_status": "completed",
            }
            context["ti"].xcom_push(key="validation_result", value=result)
        finally:
            await conn.close()

    asyncio.run(valid())


def check_quality_data(**context):
    validation_result = context["ti"].xcom_pull(
        task_ids="validation",
        key="validation_result",
    )
    if validation_result and validation_result.get("has_warning"):
        context["ti"].xcom_push(key="send_alert", value=True)
        return "send_alert_email"

    return "generate_report"


def generate_report(**context):
    records = context["ti"].xcom_pull(task_ids="load", key="records")
    duration = context["ti"].xcom_pull(task_ids="load", key="duration")

    report = {
        "date": context["ds"],
        "records_loaded": records,
        "duration_seconds": duration,
        "status": "SUCCESS",
    }

    context["ti"].xcom_push(key="report", value=report)


with DAG(
    dag_id="etl_pipeline",
    default_args=args_default,
    schedule_interval="@daily",
    description="ETL pipeline for bank data from CSV to PgSQL",
    catchup=False,
    tags=["etl", "bank"],
    max_active_runs=1,
) as dag:

    check_files = FileSensor(
        task_id="check_files",
        filepath=f"{BASE_DIR}/files/*.csv",
        fs_conn_id="fs_default",
        poke_interval=30,
        timeout=3600,
        soft_fail=False,
    )

    load_data = PythonOperator(
        task_id="load",
        python_callable=load,
        execution_timeout=timedelta(minutes=30),
    )

    validate_data = PythonOperator(
        task_id="validation",
        python_callable=validation,
    )

    check_quality = BranchPythonOperator(
        task_id="check_quality",
        python_callable=check_quality_data,
    )

    send_report = PythonOperator(
        task_id="generate_report",
        python_callable=generate_report,
    )

    send_email = EmailOperator(
        task_id="send_alert_email",
        to=["admin@bank.ru"],
        subject="Data quality alert - {{ ds }}",
        html_content="""
        <h3>Data Quality Alert</h3>
        <p>Date: {{ ds }}</p>
        <p>Records loaded: {{ ti.xcom_pull(task_ids='load', key='records') }}</p>
        <p>Error count: {{ ti.xcom_pull(task_ids='validation', key='validation_result').get('error_count') }}</p>
        """,
    )

    check_files >> load_data >> validate_data >> check_quality

    check_quality >> [send_report, send_email]
