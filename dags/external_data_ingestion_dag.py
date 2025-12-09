from datetime import datetime, timedelta
import requests
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable


def fetch_weather_data(**context):
    """Fetch weather data from NOAA API"""
    api_key = Variable.get("noaa_api_key")
    zip_code = Variable.get("hospital_zip_code", "12345")
    
    url = f"https://www.ncdc.noaa.gov/cdo-web/api/v2/data"
    params = {
        "datasetid": "GHCND",
        "locationid": f"ZIP:{zip_code}",
        "startdate": "2025-01-01",
        "enddate": "2025-01-31"
    }
    headers = {"token": api_key}
    
    response = requests.get(url, headers=headers, params=params)
    data = response.json()
    
    # Transform and store
    df = pd.DataFrame(data.get("results", []))
    # TODO: Save to database/feature store
    print(f"Fetched {len(df)} weather records")
    return data


def fetch_flu_surveillance(**context):
    """Fetch CDC FluView data"""
    # CDC FluView API endpoint
    url = "https://gis.cdc.gov/grasp/flu2/GetDataTWP"
    
    # TODO: Implement CDC API call
    print("Fetching flu surveillance data...")
    return "flu_data_fetched"


def join_external_features(**context):
    """Join external data sources with hospital data"""
    ti = context["ti"]
    
    # Pull data from previous tasks
    weather_data = ti.xcom_pull(task_ids="fetch_weather")
    flu_data = ti.xcom_pull(task_ids="fetch_flu")
    
    # TODO: Load hospital data from warehouse
    # TODO: Join on date + geography (zip/county)
    # TODO: Create features (lags, rolling averages)
    # TODO: Save to feature store
    
    print("Joined external features with hospital data")
    return "features_ready"


default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "email_on_failure": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="external_data_ingestion",
    default_args=default_args,
    description="Daily ingestion of external data sources",
    schedule_interval="0 6 * * *",  # Daily at 6 AM
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["data-ingestion", "external"],
) as dag:
    
    weather = PythonOperator(
        task_id="fetch_weather",
        python_callable=fetch_weather_data,
    )
    
    flu = PythonOperator(
        task_id="fetch_flu",
        python_callable=fetch_flu_surveillance,
    )
    
    join = PythonOperator(
        task_id="join_features",
        python_callable=join_external_features,
    )
    
    [weather, flu] >> join
