
from datetime import datetime, timezone

import requests
from pymongo import MongoClient

from airflow import DAG

try:
    # Airflow 3
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:
    # Airflow 2 compatibility
    from airflow.operators.python import PythonOperator


# ---------------- CONFIGURATION ----------------

API_URL = "https://api.frankfurter.dev/v2/rates"

MONGO_URI = (
    "mongodb://admin:admin123@mongodb:27017/"
    "?authSource=admin"
)

DATABASE_NAME = "currency_database"
COLLECTION_NAME = "exchange_rates"


# TASK 1: EXTRACT

def extract_currency_rates(**context):

    params = {
        "base": "EUR",
        "quotes": "USD,GBP,JPY"
    }

    response = requests.get(
        API_URL,
        params=params,
        timeout=30
    )

    response.raise_for_status()

    raw_data = response.json()

    print("Extracted currency rates:")
    print(raw_data)

    # Named XCom: Store extracted data
    context["ti"].xcom_push(
        key="raw_exchange_rates",
        value=raw_data
    )


# ---------------- TASK 2: TRANSFORM ----------------

def transform_exchange_rates(**context):

    # Retrieve extracted data using named XCom
    raw_data = context["ti"].xcom_pull(
        task_ids="extract_currency_rates",
        key="raw_exchange_rates"
    )

    if not raw_data:
        raise ValueError("No exchange rate data received")

    retrieved_at = datetime.now(
        timezone.utc
    ).isoformat(timespec="seconds")

    transformed_data = []

    for item in raw_data:

        record = {
            "base_currency": item["base"],
            "target_currency": item["quote"],
            "exchange_rate": item["rate"],
            "rate_date": item["date"],
            "source": "Frankfurter API",
            "retrieved_at": retrieved_at
        }

        transformed_data.append(record)

    print("Transformed currency data:")
    print(transformed_data)

    # Named XCom: Store transformed data
    context["ti"].xcom_push(
        key="transformed_exchange_rates",
        value=transformed_data
    )


# ---------------- TASK 3: LOAD ----------------

def load_into_mongodb(**context):

    # Retrieve transformed data using named XCom
    transformed_data = context["ti"].xcom_pull(
        task_ids="transform_exchange_rates",
        key="transformed_exchange_rates"
    )

    if not transformed_data:
        raise ValueError("No transformed data received")

    client = MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=10000
    )

    try:
        # Verify MongoDB connection
        client.admin.command("ping")

        db = client[DATABASE_NAME]
        collection = db[COLLECTION_NAME]

        # Avoid duplicate records on retries
        for record in transformed_data:

            collection.update_one(
                {
                    "base_currency": record["base_currency"],
                    "target_currency": record["target_currency"],
                    "rate_date": record["rate_date"]
                },
                {"$set": record},
                upsert=True
            )

        print(
            f"Successfully loaded {len(transformed_data)} "
            "records into MongoDB."
        )

    finally:
        client.close()


# ---------------- DAG DEFINITION ----------------

with DAG(
    dag_id="currency_exchange_etl",
    description="Daily currency exchange ETL using Frankfurter API and MongoDB",
    start_date=datetime(2026, 11, 12),
    schedule="@daily",
    catchup=False,
    tags=["ETL", "Currency", "MongoDB"]
) as dag:

    extract_task = PythonOperator(
        task_id="extract_currency_rates",
        python_callable=extract_currency_rates
    )

    transform_task = PythonOperator(
        task_id="transform_exchange_rates",
        python_callable=transform_exchange_rates
    )

    load_task = PythonOperator(
        task_id="load_into_mongodb",
        python_callable=load_into_mongodb
    )

    # Task execution order
    extract_task >> transform_task >> load_task