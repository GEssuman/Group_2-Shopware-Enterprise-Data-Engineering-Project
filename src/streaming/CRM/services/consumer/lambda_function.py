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

# Set up logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(handler)

# Configuration
REQUIRED_ENV_VARS = ["STREAM_NAME", "BUCKET", "DLQ_URL"]
MIN_TIMESTAMP = 0.0
MAX_TIMESTAMP = 1767225600.0
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

# AWS Clients
s3 = boto3.client("s3")
sqs = boto3.client("sqs")

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
        if cleaned["timestamp"] < MIN_TIMESTAMP or cleaned["timestamp"] > MAX_TIMESTAMP:
            raise ValueError(
                f"Invalid timestamp: {cleaned['timestamp']}, must be between {MIN_TIMESTAMP} and {MAX_TIMESTAMP}"
            )
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


def lambda_handler(event, context):
    """Lambda handler for Kinesis stream events."""
    logger.info(f"Received event: {json.dumps(event, indent=2)}")
    logger.info(f"Number of records received: {len(event.get('Records', []))}")
    logger.info("Processing Kinesis stream event")
    logger.handlers[0].flush()
    processed_count = 0
    failed_count = 0

    for record in event["Records"]:
        sequence_number = record["kinesis"]["sequenceNumber"]
        try:
            logger.debug(
                f"Raw record data for {sequence_number}: {record['kinesis']['data']}"
            )
            logger.handlers[0].flush()
            try:
                # Decode base64-encoded Kinesis data and parse JSON
                data = json.loads(
                    base64.b64decode(record["kinesis"]["data"]).decode("utf-8")
                )
            except (json.JSONDecodeError, UnicodeDecodeError, TypeError) as e:
                logger.error(f"Invalid data in record {sequence_number}: {str(e)}")
                logger.handlers[0].flush()
                send_to_dlq(
                    record["kinesis"]["data"],
                    f"Decode error: {str(e)}",
                    sequence_number,
                )
                failed_count += 1
                continue

            is_valid, error = validate_record(data)
            if not is_valid:
                logger.error(f"Validation failed for record {sequence_number}: {error}")
                logger.handlers[0].flush()
                send_to_dlq(data, error, sequence_number)
                failed_count += 1
                continue

            cleaned_data, error = clean_record(data)
            if error:
                logger.error(f"Cleaning failed for record {sequence_number}: {error}")
                logger.handlers[0].flush()
                send_to_dlq(data, error, sequence_number)
                failed_count += 1
                continue

            cleaned_data["ingestion_time"] = datetime.utcnow().isoformat()
            s3_key = f"processed/year={datetime.utcnow().year}/month={datetime.utcnow().month:02d}/day={datetime.utcnow().day:02d}/{sequence_number}.json"
            retries_s3 = 0
            while retries_s3 < MAX_RETRIES:
                try:
                    s3.put_object(
                        Bucket=BUCKET, Key=s3_key, Body=json.dumps(cleaned_data)
                    )
                    processed_count += 1
                    logger.info(f"Processed record {sequence_number} to S3: {s3_key}")
                    logger.handlers[0].flush()
                    break
                except ClientError as e:
                    retries_s3 += 1
                    if retries_s3 == MAX_RETRIES:
                        logger.error(
                            f"Failed to write record {sequence_number} to S3 after {MAX_RETRIES} retries: {str(e)}"
                        )
                        logger.handlers[0].flush()
                        send_to_dlq(
                            cleaned_data, f"S3 write error: {str(e)}", sequence_number
                        )
                        failed_count += 1
                        break
                    wait_time = min(2**retries_s3 + random.uniform(0, 0.5), 30)
                    logger.warning(
                        f"S3 write failed, retrying in {wait_time:.2f} seconds..."
                    )
                    logger.handlers[0].flush()
                    time.sleep(wait_time)

        except Exception as e:
            logger.error(f"Unexpected error in record {sequence_number}: {str(e)}")
            logger.handlers[0].flush()
            send_to_dlq(
                record["kinesis"]["data"],
                f"Unexpected error: {str(e)}",
                sequence_number,
            )
            failed_count += 1

    logger.info(
        f"Processed {processed_count} records, failed {failed_count} records in batch"
    )
    logger.handlers[0].flush()
    return {
        "statusCode": 200,
        "body": json.dumps(
            {"processed_count": processed_count, "failed_count": failed_count}
        ),
    }
