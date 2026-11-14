
from airflow import DAG
import pendulum
from datetime import datetime, timedelta

from video_stats import (
    get_playlist_id,
    get_video_ids,
    extract_video_data,
    save_to_json
)

from datawarehouse.dwh import staging_table, core_table
from dataquality.soda import yt_elt_data_quality


# Define the local timezone
local_tz = pendulum.timezone("Asia/Kolkata")


# Default arguments
default_args = {
    "owner": "dataengineers",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    # "email": "your_actual_email@example.com",
    # "retries": 1,
    # "retry_delay": timedelta(minutes=5),

    "start_date": datetime(2026, 11, 1, tzinfo=local_tz),
}


# Variables
staging_schema = "staging"
core_schema = "core"


# DAG 1: Produce JSON
with DAG(
    dag_id="produce_json",
    default_args=default_args,
    description="DAG to produce JSON file with raw data",
    schedule="0 14 * * *",
    catchup=False
) as dag:

    playlist_id = get_playlist_id()
    video_ids = get_video_ids(playlist_id)
    extract_data = extract_video_data(video_ids)
    save_to_json_task = save_to_json(extract_data)

    playlist_id >> video_ids >> extract_data >> save_to_json_task


# DAG 2: Update database
with DAG(
    dag_id="update_db",
    default_args=default_args,
    description="DAG to update staging and core database tables",
    schedule="0 15 * * *",
    catchup=False
) as dag:

    staging = staging_table()
    core = core_table()

    staging >> core


# DAG 3: Data quality
with DAG(
    dag_id="data_quality",
    default_args=default_args,
    description="DAG to check data quality on both database layers",
    schedule="0 16 * * *",
    catchup=False
) as dag:

    soda_validate_staging = yt_elt_data_quality(staging_schema)
    soda_validate_core = yt_elt_data_quality(core_schema)

    soda_validate_staging >> soda_validate_core
