import io
import os

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from databricks.sdk import WorkspaceClient
from dotenv import load_dotenv

load_dotenv()

VOLUME_BASE = "/Volumes/airflow_gold/silver/staging"
S3_PREFIX = "silver/"


class DatabricksSync:

    def __init__(self):
        self.client = WorkspaceClient(
            host=os.getenv("Databricks_HOST"),
            token=os.getenv("Databricks_PAT"),
        )

    def _clear_folder(self, path: str):
        """Recursively delete everything inside a Volume folder."""
        try:
            items = list(self.client.files.list_directory_contents(path))
        except Exception:
            return  # folder doesn't exist yet on the first run

        for item in items:
            if item.is_directory:
                self._clear_folder(item.path)
                self.client.files.delete_directory(item.path)
            else:
                self.client.files.delete(item.path)

    @staticmethod
    def _should_skip(key: str) -> bool:
        """Skip folder markers and Spark junk, but keep Delta's _delta_log files."""
        if key.endswith("/") or key.endswith("$folder$"):
            return True
        name = key.split("/")[-1]
        in_delta_log = "/_delta_log/" in key
        return name.startswith("_") and not in_delta_log  # e.g. _SUCCESS

    def sync_silver_to_volume(self, bucket: str) -> int:
        """Copy everything under silver/ (obt and obt_parquet) from S3 into the Volume."""
        hook = S3Hook(aws_conn_id="aws_s3_conn")  # same connection as bronze_layer.py

        # 1. List first, so we never wipe the Volume if S3 is empty
        keys = [
            k for k in (hook.list_keys(bucket_name=bucket, prefix=S3_PREFIX) or [])
            if not self._should_skip(k)
        ]
        if not keys:
            raise RuntimeError("No files found under silver/, did the Glue jobs write anything?")

        # 2. Clear the top-level folders that are about to be refreshed
        folders = {k[len(S3_PREFIX):].split("/")[0] for k in keys}  # {"obt", "obt_parquet"}
        for folder in folders:
            self._clear_folder(f"{VOLUME_BASE}/{folder}")

        # 3. Upload, keeping the same folder structure
        count = 0
        for key in keys:
            relative_path = key[len(S3_PREFIX):]  # e.g. obt/_delta_log/000.json
            body = hook.get_key(key, bucket_name=bucket).get()["Body"].read()
            self.client.files.upload(
                f"{VOLUME_BASE}/{relative_path}", io.BytesIO(body), overwrite=True
            )
            count += 1

        print(f"Synced folders: {sorted(folders)}")
        return count


if __name__ == "__main__":
    # Only works inside the Airflow container (needs the aws_s3_conn connection)
    obj = DatabricksSync()
    n = obj.sync_silver_to_volume("awss3bucketkaustav")
    print(f"Copied {n} files to {VOLUME_BASE}")