import json
import base64
import logging
import uuid
from datetime import datetime
from botocore.exceptions import ClientError
import boto3
import random
import os
import sys
import time
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
import io

# Set up logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(handler)

# Configuration
REQUIRED_ENV_VARS = [
    "STREAM_NAME",
    "BUCKET",
    "DLQ_URL",
    "ATHENA_WORKGROUP",
    "ATHENA_DATABASE",
    "ATHENA_TABLE",
]
MAX_RETRIES = 5
BATCH_SIZE = 100

# Validate environment variables
missing_vars = [var for var in REQUIRED_ENV_VARS if var not in os.environ]
if missing_vars:
    logger.error(f"Missing required environment variables: {', '.join(missing_vars)}")
    logger.handlers[0].flush()
    raise ValueError(
        f"Missing required environment variables: {', '.join(missing_vars)}"
    )

STREAM_NAME = os.environ["STREAM_NAME"]
BUCKET = os.environ["BUCKET"]
DLQ_URL = os.environ["DLQ_URL"]
ATHENA_WORKGROUP = os.environ["ATHENA_WORKGROUP"]
ATHENA_DATABASE = os.environ["ATHENA_DATABASE"]
ATHENA_TABLE = os.environ["ATHENA_TABLE"]

# AWS Clients
s3 = boto3.client("s3")
sqs = boto3.client("sqs")
athena = boto3.client("athena")

# Expected schema
EXPECTED_SCHEMA = {
    "customer_id": int,
    "interaction_type": str,
    "timestamp": float,
    "channel": str,
    "rating": int,
    "message_excerpt": str,
}
REQUIRED_FIELDS = ["customer_id", "interaction_type", "timestamp"]
OPTIONAL_FIELDS = ["channel", "rating", "message_excerpt"]


def validate_record(data):
    """Validate a single record against the schema and non-null requirements."""
    try:
        for field in REQUIRED_FIELDS:
            if field not in data or data[field] is None:
                return False, f"Missing or null field: {field}"
        for field, expected_type in EXPECTED_SCHEMA.items():
            if field in data and data[field] is not None:
                if not isinstance(data[field], expected_type):
                    return (
                        False,
                        f"Invalid type for {field}: expected {expected_type}, got {type(data[field])}",
                    )
        return True, None
    except Exception as e:
        return False, f"Validation error: {str(e)}"


def clean_record(data):
    """Clean the record by trimming strings, validating ranges, and normalizing nulls."""
    cleaned = data.copy()
    try:
        for field in ["interaction_type", "channel", "message_excerpt"]:
            if field in cleaned and cleaned[field] is not None:
                if isinstance(cleaned[field], str):
                    cleaned[field] = cleaned[field].strip()
                    if not cleaned[field]:
                        cleaned[field] = None
                else:
                    cleaned[field] = None
        if "rating" in cleaned and cleaned["rating"] is not None:
            if not isinstance(cleaned["rating"], int) or not (
                1 <= cleaned["rating"] <= 5
            ):
                cleaned["rating"] = None
        if cleaned["customer_id"] <= 0:
            raise ValueError(
                f"Invalid customer_id: {cleaned['customer_id']}, must be positive"
            )
        # Convert epoch timestamp (float) to datetime object for Parquet/Athena TIMESTAMP
        if "timestamp" in cleaned and cleaned["timestamp"] is not None:
            try:
                cleaned["timestamp"] = datetime.fromtimestamp(cleaned["timestamp"])
            except (ValueError, TypeError) as e:
                raise ValueError(f"Invalid timestamp: {cleaned['timestamp']}, {str(e)}")
        return cleaned, None
    except Exception as e:
        return None, f"Cleaning error: {str(e)}"


def send_to_dlq(record_data, error_message, sequence_number):
    """Send invalid record to SQS DLQ with retries, handling bytes data."""
    retries = 0
    record_data_serializable = (
        base64.b64encode(record_data).decode("utf-8")
        if isinstance(record_data, bytes)
        else record_data
    )
    while retries < MAX_RETRIES:
        try:
            sqs.send_message(
                QueueUrl=DLQ_URL,
                MessageBody=json.dumps(
                    {
                        "error": error_message,
                        "record": record_data_serializable,
                        "sequence_number": sequence_number,
                        "timestamp": datetime.utcnow().isoformat(),
                    }
                ),
            )
            logger.info(f"Sent record {sequence_number} to DLQ")
            logger.handlers[0].flush()
            return True
        except ClientError as e:
            retries += 1
            if retries == MAX_RETRIES:
                logger.error(
                    f"Failed to send record {sequence_number} to DLQ after {MAX_RETRIES} retries: {str(e)}"
                )
                logger.handlers[0].flush()
                return False
            wait_time = min(2**retries + random.uniform(0, 0.5), 30)
            logger.warning(f"DLQ send failed, retrying in {wait_time:.2f} seconds...")
            logger.handlers[0].flush()
            time.sleep(wait_time)
    return False


def write_batch_to_s3(batch, batch_id, ingestion_time):
    """Write micro-batch to S3 as Parquet with partitioning and add Athena partition."""
    try:
        now = datetime.fromisoformat(ingestion_time.replace("Z", "+00:00"))
        s3_key = (
            f"processed/year={now.year}/month={now.month:02d}/day={now.day:02d}/"
            f"hour={now.hour:02d}/{batch_id}.parquet"
        )
        partition_path = f"processed/year={now.year}/month={now.month:02d}/day={now.day:02d}/hour={now.hour:02d}/"
        # Convert batch to pandas DataFrame with explicit datetime dtype
        df = pd.DataFrame(batch)
        if "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce").dt.floor(
                "ms"
            )
        if "ingestion_time" in df.columns:
            df["ingestion_time"] = pd.to_datetime(
                df["ingestion_time"], errors="coerce"
            ).dt.floor("ms")
        # Convert DataFrame to PyArrow Table with explicit schema
        schema = pa.schema(
            [
                ("customer_id", pa.int32()),
                ("interaction_type", pa.string()),
                ("timestamp", pa.timestamp("ms")),
                ("channel", pa.string()),
                ("rating", pa.int32()),
                ("message_excerpt", pa.string()),
                ("ingestion_time", pa.timestamp("ms")),
            ]
        )
        table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
        # Write to S3 as Parquet
        buffer = io.BytesIO()
        pq.write_table(table, buffer)
        s3.put_object(Bucket=BUCKET, Key=s3_key, Body=buffer.getvalue())
        logger.info(f"Stored micro-batch {batch_id} to S3: {s3_key}")
        logger.handlers[0].flush()

        # Add Athena partition
        retries = 0
        sql = f"""
        ALTER TABLE {ATHENA_DATABASE}.{ATHENA_TABLE}
        ADD IF NOT EXISTS PARTITION (year={now.year}, month={now.month}, day={now.day}, hour={now.hour})
        LOCATION 's3://{BUCKET}/{partition_path}'
        """
        while retries < MAX_RETRIES:
            try:
                response = athena.start_query_execution(
                    QueryString=sql,
                    WorkGroup=ATHENA_WORKGROUP,
                    ResultConfiguration={
                        "OutputLocation": f"s3://{BUCKET}/athena-results/"
                    },
                )
                query_execution_id = response["QueryExecutionId"]
                logger.info(
                    f"Initiated Athena partition query for batch {batch_id}: {query_execution_id}"
                )
                logger.handlers[0].flush()
                while True:
                    status_response = athena.get_query_execution(
                        QueryExecutionId=query_execution_id
                    )
                    status = status_response["QueryExecution"]["Status"]["State"]
                    if status in ["SUCCEEDED", "FAILED", "CANCELLED"]:
                        if status != "SUCCEEDED":
                            error_msg = status_response["QueryExecution"]["Status"].get(
                                "StateChangeReason", "No error details"
                            )
                            logger.error(
                                f"Partition query failed for batch {batch_id}: {error_msg}"
                            )
                            logger.handlers[0].flush()
                        else:
                            logger.info(f"Added Athena partition for batch {batch_id}")
                            logger.handlers[0].flush()
                        break
                    time.sleep(1)
                break
            except ClientError as e:
                retries += 1
                if retries == MAX_RETRIES or "AccessDenied" in str(e):
                    logger.error(
                        f"Failed to add partition for batch {batch_id} after {retries} retries: {str(e)}"
                    )
                    logger.handlers[0].flush()
                    break
                wait_time = min(2**retries + random.uniform(0, 0.5), 30)
                logger.warning(
                    f"Athena query failed, retrying in {wait_time:.2f} seconds: {str(e)}"
                )
                logger.handlers[0].flush()
                time.sleep(wait_time)

        return s3_key  # Return s3_key regardless of partition query success
    except Exception as e:
        logger.error(f"Failed to store micro-batch {batch_id} to S3: {str(e)}")
        logger.handlers[0].flush()
        return None


def process_batch(batch, batch_id, failed_records):
    """Process a micro-batch: write to S3 as Parquet."""
    if not batch:
        logger.info("Empty batch, skipping processing")
        return True
    ingestion_time = datetime.utcnow()
    for record in batch:
        record["ingestion_time"] = ingestion_time
    s3_key = write_batch_to_s3(batch, batch_id, ingestion_time.isoformat())
    if not s3_key:
        logger.error(f"Failed to write batch {batch_id} to S3, sending to DLQ")
        for record, sequence_number in failed_records:
            send_to_dlq(record, f"S3 write error for batch {batch_id}", sequence_number)
        return False
    return True
