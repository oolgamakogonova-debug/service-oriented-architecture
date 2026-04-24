import io
import logging
from datetime import date
import boto3
from botocore.client import Config
import pandas as pd
from tenacity import retry, stop_after_attempt, wait_exponential
from app.config import (
    MINIO_ENDPOINT,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
    MINIO_BUCKET,
)

logger = logging.getLogger(__name__)


class S3Exporter:
    def __init__(self):
        self.client = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
            config=Config(signature_version="s3v4"),
            region_name="us-east-1",
        )
        self.bucket = MINIO_BUCKET
        self._ensure_bucket()

    def _ensure_bucket(self):
        try:
            self.client.head_bucket(Bucket=self.bucket)
        except Exception:
            logger.info("Creating bucket %s", self.bucket)
            try:
                self.client.create_bucket(Bucket=self.bucket)
            except Exception as e:
                logger.warning("Could not create bucket (may already exist): %s", e)

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def export_parquet(self, rows: list[dict], target_date: date) -> str:
        if not rows:
            logger.info("No rows to export for %s", target_date)
            return ""
        df = pd.DataFrame(rows)
        df["event_date"] = pd.to_datetime(df["event_date"])

        buf = io.BytesIO()
        df.to_parquet(buf, engine="pyarrow", compression="snappy", index=False)
        buf.seek(0)

        key = f"aggregates/year={target_date.year}/month={target_date.month:02d}/day={target_date.day:02d}/movies_agg.parquet"
        self.client.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=buf.getvalue(),
            ContentType="application/octet-stream",
        )
        s3_path = f"s3://{self.bucket}/{key}"
        logger.info("Exported %d rows to %s", len(rows), s3_path)
        return s3_path

    def ping(self) -> bool:
        try:
            self.client.head_bucket(Bucket=self.bucket)
            return True
        except Exception as e:
            logger.error("S3 ping failed: %s", e)
            return False