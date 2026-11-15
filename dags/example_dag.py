import glob
import json
import os
import shutil
import time
from airflow.utils.email import send_email
from datetime import datetime, timedelta
from airflow.decorators import task
from airflow.exceptions import AirflowSkipException, AirflowException
import pandas as pd
import pendulum
from airflow import DAG
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.http_hook import HttpHook
from airflow.providers.sftp.hooks.sftp import SFTPHook
import logging

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)


args = {
    "owner": "vadim",
    "start_date": pendulum.datetime(2026, 3, 1),
}


def func_fetch_sales_from_api(**context):
    api_key = Variable.get("retail_api_key")
    date = context["execution_date"]
    prev_date = (date - timedelta(days=1)).strftime("%Y-%m-%d")
    hook = HttpHook(method="GET", http_conn_id="api_retail")
    response = hook.run(
        endpoint="v1/sales",
        data={"date": prev_date},
        headers={"X-API-Key": api_key},
        extra_options={"check_response": True},
    )
    list_sales = response.json()
    context["ti"].xcom_push(key="fetch_sales", value=list_sales)
    context["ti"].xcom_push(key="prev_date", value=prev_date)
    return list_sales


def func_load_products_from_s3(**context):
    hook = S3Hook(aws_conn_id="retail_data")
    temp_path = "/tmp/products"
    os.makedirs(temp_path, exist_ok=True)
    local_path = f"{temp_path}/products.csv"
    hook.download_file(
        key="products/products.csv",
        bucket_name="retail-data",
        local_path=local_path,
    )
    df = pd.read_csv(local_path)
    products = df[["product_id", "product_name", "category"]].to_dict(orient="records")
    context["ti"].xcom_push(key="load_products", value=products)
    return products


def func_transform_and_join(**context):
    list_sales = context["ti"].xcom_pull(
        task_ids="fetch_sales_from_api",
        key="fetch_sales",
    )
    products = context["ti"].xcom_pull(
        task_ids="load_products_from_s3",
        key="load_products",
    )
    df_sales = pd.DataFrame(list_sales)
    df_products = pd.DataFrame(products)
    joined_sales_products = pd.merge(
        left=df_sales,
        right=df_products,
        left_on=df_sales,
        on="product_id",
        how="left",
    )
    joined_sales_products["total_amount"] = (
        joined_sales_products["quantity"] * joined_sales_products["price"]
    )
    current_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    joined_sales_products["load_timestamp"] = current_date
    sales_products = joined_sales_products.to_dict(orient="records")
    context["ti"].xcom_push(key="transform_and_join", value=sales_products)
    return sales_products


def func_load_to_postgres(**context):
    sales_products = context["ti"].xcom_pull(
        task_ids="transform_and_join",
        key="transform_and_join",
    )
    hook = PostgresHook(postgres_conn_id="con_sales")
    cursor = hook.get_cursor()
    inserted = 0
    for sp in sales_products:
        cursor.execute(
            """
            insert into fact_sales (
                sale_id, 
                product_id, 
                product_name, 
                category, 
                quantity, 
                price, 
                total_amount, 
                sale_date,
                load_timestamp
        ) values (%s, %s, %s, %s, %s, %s, %s, %s, %s) on conflict (sale_id) do update set 
            quantity = excluded.quantity, price = excluded.price, total_amount = excluded.total_amount
            returning (xmax = 0) as inserted
        """,
            (
                sp["sale_id"],
                sp["product_id"],
                sp["product_name"],
                sp["category"],
                sp["quantity"],
                sp["price"],
                sp["total_amount"],
                sp["sale_date"],
                sp["load_timestamp"],
            ),
        )
        if cursor.fetchone()[0]:
            inserted += 1

    context["ti"].xcom_push(key="load_to_postgres", value=inserted)
    hook.conn.commit()
    cursor.close()
    return inserted


def func_validate_row_count(**context):
    list_api_sales = context["ti"].xcom_pull(
        task_ids="fetch_sales_from_api",
        key="fetch_sales",
    )
    inserted_sales = context["ti"].xcom_pull(
        task_ids="load_to_postgres",
        key="load_to_postgres",
    )
    if len(list_api_sales) != inserted_sales:
        log.warning(
            f"the number of uploaded api entries is {len(list_api_sales)}"
            f" and inserted into the database is {inserted_sales}"
        )


def func_send_success_notification(**context):
    yesterday = context["ti"].xcom_pull(
        task_ids="fetch_sales_from_api",
        key="prev_date",
    )
    count = context["ti"].xcom_pull(
        task_ids="load_to_postgres",
        key="load_to_postgres",
    )
    send_email(
        to=["analysts@company.com"],
        subject="Info is about inserted",
        html_content=f"Загружено {count} продаж за {yesterday}",
    )


def info_failure_callback(context):
    task_id = context["task_instance"].task_id
    dag_id = context["dag_run"].dag_id
    execution_date = context["execution_date"]
    exception = context.get("exception")
    send_email(
        to=["admin@company.com"],
        subject=f"🚨 DAG {dag_id} упал в {execution_date}",
        html_content=f"<p>The task {task_id} was dropped due to {exception}</p>",
    )


def func_cleanup(**context):
    shutil.rmtree("/tmp/products", ignore_errors=True)


with DAG(
    dag_id="etl_pipeline",
    schedule_interval="0 6 * * *",
    default_args=args,
    catchup=False,
    on_failure_callback=info_failure_callback,
) as dag:

    fetch_sales_from_api = PythonOperator(
        task_id="fetch_sales_from_api",
        python_callable=func_fetch_sales_from_api,
        retries=2,
    )
    load_products_from_s3 = PythonOperator(
        task_id="load_products_from_s3",
        python_callable=func_load_products_from_s3,
        retries=2,
    )
    transform_and_join = PythonOperator(
        task_id="transform_and_join",
        python_callable=func_transform_and_join,
    )
    load_to_postgres = PythonOperator(
        task_id="load_to_postgres",
        python_callable=func_load_to_postgres,
    )
    validate_row_count = PythonOperator(
        task_id="validate_row_count",
        python_callable=func_validate_row_count,
    )
    send_success_notification = PythonOperator(
        task_id="send_success_notification",
        python_callable=func_send_success_notification,
    )
    cleanup = PythonOperator(
        task_id="cleanup",
        python_callable=func_cleanup,
    )
    [fetch_sales_from_api, load_products_from_s3] >> transform_and_join
    (
        transform_and_join
        >> load_to_postgres
        >> validate_row_count
        >> send_success_notification
    )
    send_success_notification >> cleanup
