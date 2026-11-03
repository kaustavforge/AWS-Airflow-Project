# from datetime import datetime, timedelta
# from airflow.sdk import dag, task
# import os
# import sys

# sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

# from utils.bronze_layer import BronzeLayer
# from utils.silver_layer import SilverLayer
# from utils.gold_layer import GoldLayer

# # -------------------------------- CODE ----------------------------------- #

# @dag(
#     schedule='@daily',
#     is_paused_upon_creation=False
# )
# def aws_project():

#     # Task to extract data from API and load it to S3
#     @task.python(retries=3, retry_delay=timedelta(seconds=5))
#     def extract_load_s3():
#         # Fethcing data from API and loading it to S3 using Loop
#         urls = ["https://raw.githubusercontent.com/anshlambagit/ApacheAirflow/refs/heads/main/bookings.csv",
#                 "https://raw.githubusercontent.com/anshlambagit/ApacheAirflow/refs/heads/main/passengers.csv",
#                 "https://raw.githubusercontent.com/anshlambagit/ApacheAirflow/refs/heads/main/airports.csv"]

#         obj = BronzeLayer()

#         folder_name = datetime.now().strftime("%Y-%m-%d")

#         for url in urls:
#             fetched_data = obj.ingest_data_api(url)
#             obj.put_data_s3_hook("awss3bucketansh",f"bronze/{folder_name}/{url.split('/')[-1]}",fetched_data)

#         return folder_name

#     # Task to trigger Spark job in AWS Glue to transform the data and load it to S3
#     @task.python(retries=3, retry_delay=timedelta(seconds=5))
#     def transform_load_s3(ti):
#         # Fetching the last load date from previous task using XCom
#         last_load_date = ti.xcom_pull(task_ids='extract_load_s3', key='return_value')

#         obj = SilverLayer()

#         # Triggering the Spark job in AWS Glue to transform the data and load it to S3
#         job_run_id = obj.trigger_spark_job("silver_layer",last_load_date)
#         print(f"Glue Job triggered with JobRunId: {job_run_id}")

#     # Task to trigger Spark job in AWS Glue to transform the data and load it in the parquet format to S3
#     @task.python(retries=3, retry_delay=timedelta(seconds=5))
#     def transform_load_s3_parquet(ti):
#         # Fetching the last load date from previous task using XCom
#         last_load_date = ti.xcom_pull(task_ids='extract_load_s3', key='return_value')

#         obj = SilverLayer()

#         # Triggering the Spark job in AWS Glue to transform the data and load it to S3 in parquet format
#         job_run_id = obj.trigger_spark_job("silver_layer_athena",last_load_date)
#         print(f"Glue Job triggered with JobRunId: {job_run_id}")

#     # Task to trigger the Glue crawler to update the Glue catalog
#     @task.python(retries=3, retry_delay=timedelta(seconds=5))
#     def trigger_crawler():
#         obj = SilverLayer()
#         craller_response = obj.trigger_crawler("crawler_silver")
#         print(craller_response)


#     # Task to trigger the Databricks job to transform the data and load it to the gold layer
#     @task.python(retries=3, retry_delay=timedelta(seconds=5))
#     def trigger_databricks_job():
#         obj = GoldLayer()
#         response = obj.trigger_databricks_job(job_id=1062568820513153)  # Replace with your actual job ID
#         print(response)

#     # Task Dependency
#     extract_load_s3 = extract_load_s3()
#     transform_load_s3 = transform_load_s3()
#     transform_load_s3_parquet = transform_load_s3_parquet()
#     trigger_crawler = trigger_crawler()
#     trigger_databricks_job = trigger_databricks_job()

#     extract_load_s3 >> [transform_load_s3, transform_load_s3_parquet] >> trigger_crawler >> trigger_databricks_job


# aws_project_dag = aws_project()

from datetime import timedelta
from airflow.sdk import dag, task, get_current_context
import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from utils.bronze_layer import BronzeLayer
from utils.silver_layer import SilverLayer
from utils.databricks_sync import DatabricksSync
from utils.gold_layer import GoldLayer

# -------------------------------- CODE ----------------------------------- #


@dag(
    schedule="@daily",
    is_paused_upon_creation=False,
    # catchup=False  # Added to prevent accidental backfills
)
def aws_project():
    # Task to extract data from API and load it to S3
    @task(retries=3, retry_delay=timedelta(seconds=5))
    def extract_load_s3():
        urls = [
            "https://raw.githubusercontent.com/anshlambagit/ApacheAirflow/refs/heads/main/bookings.csv",
            "https://raw.githubusercontent.com/anshlambagit/ApacheAirflow/refs/heads/main/passengers.csv",
            "https://raw.githubusercontent.com/anshlambagit/ApacheAirflow/refs/heads/main/airports.csv",
        ]

        obj = BronzeLayer()

        # Airflow 3.0 Best Practice: Get the logical date string (YYYY-MM-DD)
        context = get_current_context()
        folder_name = context["ds"]

        for url in urls:
            fetched_data = obj.ingest_data_api(url)
            # Updated to use the S3Hook method defined in your BronzeLayer class
            obj.put_data_s3_hook(
                "awss3bucketkaustav",
                f"bronze/{folder_name}/{url.split('/')[-1]}",
                fetched_data,
            )

        return folder_name

    # Task to trigger Spark job in AWS Glue to transform the data and load it to S3
    @task(retries=3, retry_delay=timedelta(seconds=5))
    def transform_load_s3(last_load_date: str):
        # last_load_date is automatically resolved via TaskFlow XCom
        obj = SilverLayer()

        job_run_id = obj.trigger_spark_job("silver_layer", last_load_date)
        print(f"Glue Job triggered with JobRunId: {job_run_id}")

    # Task to trigger Spark job in AWS Glue to transform the data and load it in the parquet format to S3
    @task(retries=3, retry_delay=timedelta(seconds=5))
    def transform_load_s3_parquet(last_load_date: str):
        # last_load_date is automatically resolved via TaskFlow XCom
        obj = SilverLayer()

        job_run_id = obj.trigger_spark_job("silver_layer_athena", last_load_date)
        print(f"Glue Job triggered with JobRunId: {job_run_id}")

    # Task to trigger the Glue crawler to update the Glue catalog
    @task(retries=3, retry_delay=timedelta(seconds=5))
    def trigger_crawler():
        obj = SilverLayer()
        crawler_response = obj.trigger_crawler("crawler_silver")
        print(crawler_response)

    # Task to copy the Silver parquet files from S3 into the Databricks Volume
    @task(retries=2, retry_delay=timedelta(seconds=10))
    def sync_silver_to_databricks():
        obj = DatabricksSync()
        n = obj.sync_silver_to_volume("awss3bucketkaustav")
        print(f"Copied {n} files to the Databricks volume")

    # Task to trigger the Databricks job to transform the data and load it to the gold layer
    @task(retries=1, retry_delay=timedelta(seconds=10))
    def trigger_gold_job():
        obj = GoldLayer()
        print(obj.trigger_databricks_job())

    # ------------------- Task Instantiation & Dependencies ------------------- #

    # 1. Instantiate the extract task
    extract_task = extract_load_s3()

    # 2. Instantiate transform tasks by passing the extract task output directly.
    # Passing 'extract_task' implicitly sets up XCom pull AND the dependency (extract_task >> transform_task)
    transform_task = transform_load_s3(last_load_date=extract_task)
    transform_parquet_task = transform_load_s3_parquet(last_load_date=extract_task)

    # 3. Instantiate remaining tasks
    crawler_task = trigger_crawler()
    sync_task = sync_silver_to_databricks()
    gold_task = trigger_gold_job()

    # 4. Define explicit dependencies for tasks that don't exchange data
    [transform_task, transform_parquet_task] >> crawler_task >> sync_task >> gold_task


aws_project_dag = aws_project()
