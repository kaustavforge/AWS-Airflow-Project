# 🚀 End-to-End Data Engineering Pipeline: AWS, Airflow & Databricks

This repository contains the complete setup and orchestration guide for an end-to-end data engineering pipeline following the **Medallion Architecture** (Bronze $\rightarrow$ Silver $\rightarrow$ Gold). The pipeline extracts raw data, processes it through AWS S3 and AWS Glue (using Spark and Delta Lake), catalogs it for Amazon Athena, and finally syncs it to Databricks for Gold-layer business analytics—all fully automated and orchestrated using **Apache Airflow** running in Docker.

## 🏗️ Architecture Overview

1. **Bronze Layer (Raw Ingestion):** Python scripts orchestrated by Airflow read raw CSV data from GitHub using `pandas` and ingest it into an AWS S3 Bronze folder.
2. **Silver Layer (Transformation & Cataloging):** AWS Glue (PySpark) jobs process raw data into both **Delta** and **Parquet** formats (One Big Table - OBT). AWS Glue Crawlers then catalog the Parquet data for querying in Amazon Athena.
3. **Gold Layer (Analytics & BI):** Due to AWS Free Tier limitations with Databricks External Locations, a custom Python sync script transfers Silver data from S3 to a Databricks Unity Catalog Staging Volume. A Databricks Workflow then processes this data into final Gold layer tables for business use cases.

## 🛠️ Phase 1: AWS S3 & Local Airflow Setup

### 1.1 Create S3 Bucket

1. Log in to the AWS Management Console and navigate to **S3**.
2. Create a new bucket named: `awss3bucketkaustav`.
3. Inside this bucket, create a folder named `bronze`.

### 1.2 Initialize Airflow Project Directory

Create your main project directory on your local machine and set up the following structure:

```text
airflow-project/
├── config/
├── dags/
│   └── aws_project.py
├── data/
├── logs/
├── plugins/
├── utils/
│   ├── __init__.py
│   ├── bronze_layer.py
│   ├── silver_layer.py
│   ├── databricks_sync.py
│   └── gold_layer.py
├── .env
├── docker-compose.yaml
├── Dockerfile
└── requirements.txt
```

### 1.3 Configure Custom Dockerfile & Requirements

To install essential Python packages inside the Airflow containers, set up your `requirements.txt` and `Dockerfile`, and update `docker-compose.yaml` to build the custom image.

1. Add the following dependencies to `requirements.txt`:

   ```text
   boto3
   pandas
   requests
   dotenv
   databricks-sdk
   ```

2. Add the following configuration to your `Dockerfile`:

   ```dockerfile
   FROM apache/airflow:3.3.2

   USER root

   RUN apt-get update && apt-get install -y gcc && apt-get clean

   USER airflow 

   COPY requirements.txt /requirements.txt
   RUN pip install --no-cache-dir -r /requirements.txt
   ```

3. Open `docker-compose.yaml` and uncomment the `build: .` line (and comment out the default `image:` line if required) under `x-airflow-common` so Docker builds from your local `Dockerfile`:

   ```yaml
   x-airflow-common:
     &airflow-common
     # image: ${AIRFLOW_IMAGE_NAME:-apache/airflow:3.3.2}
     build: .
   ```

### 1.4 Start Docker Containers

Open Docker Desktop, launch PowerShell or Command Prompt in your project directory, and run:

```bash
docker-compose up -d --build
```

## 🥉 Phase 2: Bronze Layer (Data Ingestion)

### 2.1 Configure AWS IAM User for S3 Access

1. In the AWS Console, go to **IAM** $\rightarrow$ **Users** $\rightarrow$ **Create user** and name it `kaustavvs`.
2. Choose **Attach policies directly**, search for `AmazonS3FullAccess`, select it, and click **Create user**.
3. Open the newly created user `kaustavvs`, navigate to the **Security credentials** tab, and scroll to **Access keys**.
4. Click **Create access key**, select **Other**, and generate the key.
5. Copy the **Access Key ID** and **Secret Access Key** and add them to your `.env` file.

### 2.2 Build Bronze Layer Ingestion Script

1. Inside the `utils/` folder, ensure you have an empty `__init__.py` file.
2. Create `bronze_layer.py` and write a Python function using `pandas` and `boto3` (without using an Airflow S3 Hook initially) to read CSV files from GitHub and upload them directly to `s3://awss3bucketkaustav/bronze/`.
3. Run `bronze_layer.py` locally first to verify that files upload to the S3 `bronze` folder properly.

### 2.3 Configure Airflow Volumes & S3 Connection

1. Open `docker-compose.yaml` and mount your project folders under the `volumes:` section:

   ```yaml
   volumes:
     - ${AIRFLOW_PROJ_DIR:-.}/dags:/opt/airflow/dags
     - ${AIRFLOW_PROJ_DIR:-.}/utils:/opt/airflow/utils
     - ${AIRFLOW_PROJ_DIR:-.}/data:/opt/airflow/data
     - ${AIRFLOW_PROJ_DIR:-.}/plugins:/opt/airflow/plugins
     - ${AIRFLOW_PROJ_DIR:-.}/config:/opt/airflow/config
   ```

2. *(Optional)* If you choose to upload files using Airflow's native `S3Hook`, add your connection string under the environment variables in `docker-compose.yaml`:

   ```yaml
   # AIRFLOW CONNECTION FOR AWS S3
   AIRFLOW_CONN_AWS_S3_CONN: "aws://YOUR_ACCESS_KEY:YOUR_SECRET_KEY@?region_name=ap-southeast-2"
   ```

   You can verify the connection in PowerShell using:

   ```bash
   docker compose exec airflow-scheduler airflow connections get aws_s3_conn
   ```

3. Restart your Docker containers to apply the changes:

   ```bash
   docker-compose down
   docker-compose up -d
   ```

### 2.4 Create Initial Airflow DAG

1. Inside `dags/aws_project.py`, write your first task using the `@task.python` decorator (`extract_load_s3`) to call your Bronze ingestion function.
2. Open the Airflow Webserver (`http://localhost:8080`), unpause the DAG, and trigger a run to confirm it uploads files to S3.

## 🥈 Phase 3: Silver Layer (AWS Glue, Delta Lake & Parquet)

### 3.1 Create IAM Role for AWS Glue

1. Go to **IAM** $\rightarrow$ **Roles** $\rightarrow$ **Create role**.
2. Select **AWS service** and choose **Glue** as the use case. Click **Next**.
3. Attach the following managed policies:
   * `AmazonS3FullAccess`
   * `AWSGlueConsoleFullAccess`
   * `AWSGlueServiceRole`
   * `CloudWatchLogsFullAccess`
4. Name the role `sparkaccess` and click **Create role**.
5. Open the `sparkaccess` role, click **Add permissions** $\rightarrow$ **Create inline policy**, switch to the **JSON** tab, and paste the following (replace the account ID with your own):

   ```json
   {
     "Version": "2012-10-17",
     "Statement": [
       {
         "Effect": "Allow",
         "Action": "iam:PassRole",
         "Resource": "arn:aws:iam::0559036974xx:role/sparkaccess"
       }
     ]
   }
   ```

6. Click **Next**, name the policy `sparkinlinepolicy`, and click **Create policy**.

### 3.2 Update IAM User Permissions for Glue Triggering

To allow your local code/Airflow user (`kaustavvs`) to trigger Glue jobs:

1. Go to **IAM** $\rightarrow$ **Users** $\rightarrow$ `kaustavvs` $\rightarrow$ **Add permissions** $\rightarrow$ **Attach policies directly**.
2. Attach `AWSGlueConsoleFullAccess`, `AWSGlueServiceRole`, and `CloudWatchLogsFullAccess`.
3. Click **Add permissions** $\rightarrow$ **Create inline policy**, switch to **JSON**, and paste:

   ```json
   {
     "Version": "2012-10-17",
     "Statement": [
       {
         "Effect": "Allow",
         "Action": "glue:StartJobRun",
         "Resource": "arn:aws:glue:ap-southeast-2:055903697491:job/silver_layer"
       },
       {
         "Effect": "Allow",
         "Action": "iam:PassRole",
         "Resource": "arn:aws:iam::055903697491:role/sparkaccess"
       }
     ]
   }
   ```

4. Name the policy and click **Create policy**.

### 3.3 Create Glue Spark Job 1: Delta Format (`silver-layer`)

1. Go to **AWS Glue** $\rightarrow$ **Visual ETL** $\rightarrow$ **Script editor** (Spark) and create a fresh script.
2. Go to the **Job details** tab:
   * **Name:** `silver-layer`
   * **IAM Role:** `sparkaccess`
3. Scroll down to **Advanced properties** $\rightarrow$ **Job parameters** and add the following key-value pairs:
   * `--load_date`: `<today's_date>`
   * `--conf`: `spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog`
   * `--datalake-formats`: `delta`
4. Click **Save**.
5. In the **Script** tab, include `'load_date'` in the `getResolvedOptions` arguments list alongside `'JOB_NAME'`. Add your PySpark code to read the Bronze files, join/transform them into a single table (OBT), and write the output in **Delta** format to the S3 `silver` folder.
6. Click **Run** to test the job manually.

### 3.4 Automate Silver Layer in VS Code & Airflow

1. In VS Code, create `utils/silver_layer.py` and write a function `trigger_spark_job()` using `boto3` to start the Glue job, organize S3 partition folders by date, and clean up temporary directories.
2. Wait \~5 minutes for IAM permissions to propagate, then test `silver_layer.py` locally.
3. In `dags/aws_project.py`, add the task `transform_load_s3` calling this function. Delete the test output folder (`obt`) from S3, trigger the DAG from Airflow, and verify the pipeline runs Bronze $\rightarrow$ Silver automatically.

### 3.5 Create Glue Spark Job 2: Parquet Format (`silver_layer_athena`)

1. In **AWS Glue**, create a second script job named `silver_layer_athena` using the `sparkaccess` role and the same job parameters (`--load_date`, `--conf`, `--datalake-formats`).
2. Write the PySpark transformation script to output the joined table in **Parquet** format (`s3://awss3bucketkaustav/silver/obt_parquet/`). Run it once to test.
3. In `dags/aws_project.py`, create a new task `transform_load_s3_parquet` and set up parallel task dependencies:

   ```text
   extract_load_s3 ──┬── transform_load_s3 (Delta)
                     └── transform_load_s3_parquet (Parquet)
   ```

4. Clean the `silver` folder in S3 and trigger the Airflow DAG to test parallel execution.

## 🔍 Phase 4: AWS Glue Crawler & Amazon Athena

### 4.1 Set Up Glue Data Catalog & Crawler

1. Go to **AWS Glue** $\rightarrow$ **Data Catalog** $\rightarrow$ **Databases** $\rightarrow$ **Add database**. Name it `silver_db` and create it.
2. Go to **Crawlers** $\rightarrow$ **Create crawler**:
   * **Name:** `crawler_silver`
   * **Data source:** Browse S3 and select `s3://awss3bucketkaustav/silver/obt-parquet/`
   * **IAM role:** `sparkaccess`
   * **Target database:** `silver_db`
   * **Table prefix:** `obt-parquet`
3. Create and **Run** the crawler. Once complete, verify that the table `obt-parquetobt_parquet` appears under **Tables**.

### 4.2 Query Data in Amazon Athena

1. In your S3 bucket (`awss3bucketkaustav`), create a new folder named `athena` to store query results.
2. Open **Amazon Athena**, go to **Settings** $\rightarrow$ **Manage**, browse and select `s3://awss3bucketkaustav/athena/`, and click **Save**.
3. Return to the Query Editor and run standard SQL queries against `silver_db` to verify your cataloged Parquet data.

### 4.3 Automate Crawler in Airflow

1. Add the Boto3 crawler trigger steps into `utils/silver_layer.py` and test locally.
2. In `dags/aws_project.py`, create the task `trigger_crawl` and set it downstream of `transform_load_s3_parquet`.

## 🥇 Phase 5: Gold Layer (Databricks Integration & Orchestration)

> **💡 Note on AWS Free Tier Workaround:** Connecting Databricks Unity Catalog directly to S3 via External Locations requires paid AWS/Databricks tier permissions. To bypass this limitation on the Free Tier, we create a managed staging volume in Databricks and use a Python script (`databricks_sync.py`) to programmatically sync processed Silver files from S3 directly into Databricks Unity Catalog.

### 5.1 Configure Databricks Unity Catalog & Staging Volume

1. In your Databricks Workspace, create a folder named `airflow_project`.
2. Inside this folder, create a notebook named `create_catalogue` and run SQL commands to build the following structure:
   * **Catalog:** `airflow_gold`
   * **Schemas:** `silver` (with a managed **Staging Volume**) and `gold`.

### 5.2 Sync S3 Silver Data to Databricks

1. Generate a Databricks **Personal Access Token (PAT)** and add both your `DATABRICKS_HOSTNAME` and `DATABRICKS_PAT` to your `.env` file.
2. Create `utils/databricks_sync.py` to download processed files from S3 and upload them to your Databricks Unity Catalog `silver` staging volume.
3. In `dags/aws_project.py`, add the `sync_silver_to_databricks()` task and link it downstream of your Silver tasks. Trigger the DAG to verify the sync.

### 5.3 Build & Automate Gold Layer Databricks Job

1. In Databricks (`airflow_project` folder), create a notebook named `gold_layer`. Write PySpark/SQL queries to read from the Silver staging volume, build aggregated business-level tables, and save them into the `airflow_gold.gold` schema.
2. Go to **Databricks Workflows (Jobs)** and create a new job named `gold_layer` pointing to your `gold_layer` notebook.
3. Copy the **Job ID** and add it to your `.env` file.
4. In VS Code, create `utils/gold_layer.py` to trigger the Databricks job via API/SDK.
5. In `dags/aws_project.py`, add the final task `trigger_gold_job()` downstream of `sync_silver_to_databricks()`.

## ✅ Phase 6: Final End-to-End Pipeline Test

To verify complete automation from scratch:

1. **Clean Environments:**
   * Delete all files inside the `bronze` and `silver` folders in your AWS S3 bucket.
   * Drop existing files from the `silver` staging volume and tables from the `gold` schema in Databricks.
2. **Trigger Pipeline:**
   * Go to the Airflow UI (`http://localhost:8080`) and trigger the `aws_project` DAG.
3. **Final DAG Flow:**

   ```text
                                ┌── transform_load_s3 (Delta) ───────────┐
   extract_load_s3 (Bronze) ────┤                                        ├──> sync_silver_to_databricks ──> trigger_gold_job
                                └── transform_load_s3_parquet ──> trigger_crawl
   ```

4. Verify that S3 folders, Glue Catalog tables, and Databricks Gold tables are all populated automatically. **Project Complete! 🎉**