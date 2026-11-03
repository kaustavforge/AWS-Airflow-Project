# from databricks.sdk import WorkspaceClient
# import os
# from dotenv import load_dotenv
# load_dotenv()


# class GoldLayer:

#     def __init__(self):
#         pass 

#     def trigger_databricks_job(self, job_id: int):
        
#         databricks_pat = os.getenv("Databricks_PAT")
#         databricks_host = os.getenv("Databricks_HOST")
        
#         client = WorkspaceClient(
#             host=databricks_host,
#             token=databricks_pat
#         )

#         # Trigger the Databricks job
#         run = client.jobs.run_now(job_id=job_id)
#         return f"Triggered Databricks job with ID: {run.run_id}"
    

# if __name__ == "__main__":
#     gold_layer = GoldLayer()
#     response = gold_layer.trigger_databricks_job(job_id=1062568820513153)  # Replace with your actual job ID
#     print(response)

from datetime import timedelta
import os

from databricks.sdk import WorkspaceClient
from dotenv import load_dotenv

load_dotenv()


class GoldLayer:

    def __init__(self):
        pass

    def trigger_databricks_job(self, job_id: int | None = None):

        # Use the job_id passed in, otherwise fall back to the one in .env
        if job_id is None:
            job_id_env = os.getenv("Databricks_JOB_ID")
            if not job_id_env:
                raise ValueError("Databricks_JOB_ID is not set in .env")
            job_id = int(job_id_env)

        client = WorkspaceClient(
            host=os.getenv("Databricks_HOST"),
            token=os.getenv("Databricks_PAT"),
        )

        # Trigger the Databricks job and wait until it finishes
        run = client.jobs.run_now(job_id=job_id).result(timeout=timedelta(minutes=30))

        state = run.state.result_state.value if run.state and run.state.result_state else "UNKNOWN"
        if state != "SUCCESS":
            raise RuntimeError(f"Databricks run {run.run_id} ended with state {state}")

        return f"Databricks job {job_id} succeeded (run {run.run_id})"


if __name__ == "__main__":
    gold_layer = GoldLayer()
    print(gold_layer.trigger_databricks_job())