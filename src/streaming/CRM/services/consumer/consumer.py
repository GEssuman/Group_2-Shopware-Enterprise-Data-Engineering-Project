import boto3
import json
import base64
import logging
import time
import uuid
from datetime import datetime
from botocore.exceptions import ClientError
from dotenv import load_dotenv
import os
import random

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Load environment variables (optional, for local testing)
load_dotenv()

# Configuration
REQUIRED_ENV_VARS = ["STREAM_NAME", "BUCKET", "DLQ_URL"]
POLL_INTERVAL_SECONDS = 10
MIN_TIMESTAMP = 0.0
MAX_TIMESTAMP = 1767225600.0
MAX_RETRIES = 5
BATCH_SIZE = 100

# Validate environment variables
missing_vars = [var for var in REQUIRED_ENV_VARS if os.getenv(var) is None]
if missing_vars:
    logger.error(f"Missing required environment variables: {', '.join(missing_vars)}")
    raise ValueError(
        f"Missing required environment variables: {', '.join(missing_vars)}"
    )

STREAM_NAME = os.getenv("STREAM_NAME")
BUCKET = os.getenv("BUCKET")
DLQ_URL = os.getenv("DLQ_URL")

# AWS Clients
kinesis = boto3.client("kinesis")
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
            return True
        except ClientError as e:
            retries += 1
            if retries == MAX_RETRIES:
                logger.error(
                    f"Failed to send record {sequence_number} to DLQ after {MAX_RETRIES} retries: {str(e)}"
                )
                return False
            wait_time = min(2**retries + random.uniform(0, 0.5), 30)
            logger.warning(f"DLQ send failed, retrying in {wait_time:.2f} seconds...")
            time.sleep(wait_time)
    return False


def process_records():
    """Poll Kinesis stream and process records with retries."""
    retries = 0
    while retries < MAX_RETRIES:
        try:
            response = kinesis.describe_stream(StreamName=STREAM_NAME)
            shard_id = response["StreamDescription"]["Shards"][0]["ShardId"]
            iterator_response = kinesis.get_shard_iterator(
                StreamName=STREAM_NAME,
                ShardId=shard_id,
                ShardIteratorType="TRIM_HORIZON",
            )
            shard_iterator = iterator_response["ShardIterator"]
            break
        except ClientError as e:
            retries += 1
            if retries == MAX_RETRIES:
                logger.error(
                    f"Failed to initialize Kinesis shard iterator after {MAX_RETRIES} retries: {str(e)}"
                )
                return False
            wait_time = min(2**retries + random.uniform(0, 0.5), 30)
            logger.warning(
                f"Kinesis initialization failed, retrying in {wait_time:.2f} seconds..."
            )
            time.sleep(wait_time)

    while True:
        try:
            response = kinesis.get_records(
                ShardIterator=shard_iterator, Limit=BATCH_SIZE
            )
            shard_iterator = response["NextShardIterator"]
            records = response["Records"]

            processed_count = 0
            failed_count = 0

            for record in records:
                sequence_number = record["SequenceNumber"]
                try:
                    logger.debug(
                        f"Raw record data for {sequence_number}: {record['Data']}"
                    )
                    try:
                        # Directly load JSON from the raw bytes from Kinesis.
                        data = json.loads(record["Data"])
                    except (
                        json.JSONDecodeError,
                        UnicodeDecodeError,
                    ) as e:
                        logger.error(
                            f"Invalid data in record {sequence_number}: {str(e)}"
                        )
                        send_to_dlq(
                            record["Data"], f"Decode error: {str(e)}", sequence_number
                        )
                        failed_count += 1
                        continue

                    is_valid, error = validate_record(data)
                    if not is_valid:
                        logger.error(
                            f"Validation failed for record {sequence_number}: {error}"
                        )
                        send_to_dlq(data, error, sequence_number)
                        failed_count += 1
                        continue

                    cleaned_data, error = clean_record(data)
                    if error:
                        logger.error(
                            f"Cleaning failed for record {sequence_number}: {error}"
                        )
                        send_to_dlq(data, error, sequence_number)
                        failed_count += 1
                        continue

                    cleaned_data["ingestion_time"] = datetime.utcnow().isoformat()
                    s3_key = f"year={datetime.utcnow().year}/month={datetime.utcnow().month:02d}/day={datetime.utcnow().day:02d}/{sequence_number}.json"
                    retries_s3 = 0
                    while retries_s3 < MAX_RETRIES:
                        try:
                            s3.put_object(
                                Bucket=BUCKET, Key=s3_key, Body=json.dumps(cleaned_data)
                            )
                            processed_count += 1
                            logger.info(
                                f"Processed record {sequence_number} to S3: {s3_key}"
                            )
                            break
                        except ClientError as e:
                            retries_s3 += 1
                            if retries_s3 == MAX_RETRIES:
                                logger.error(
                                    f"Failed to write record {sequence_number} to S3 after {MAX_RETRIES} retries: {str(e)}"
                                )
                                send_to_dlq(
                                    cleaned_data,
                                    f"S3 write error: {str(e)}",
                                    sequence_number,
                                )
                                failed_count += 1
                                break
                            wait_time = min(2**retries_s3 + random.uniform(0, 0.5), 30)
                            logger.warning(
                                f"S3 write failed, retrying in {wait_time:.2f} seconds..."
                            )
                            time.sleep(wait_time)

                except Exception as e:
                    logger.error(
                        f"Unexpected error in record {sequence_number}: {str(e)}"
                    )
                    send_to_dlq(
                        record["Data"], f"Unexpected error: {str(e)}", sequence_number
                    )
                    failed_count += 1

            logger.info(
                f"Processed {processed_count} records, failed {failed_count} records in batch"
            )
            time.sleep(POLL_INTERVAL_SECONDS)

        except ClientError as e:
            logger.error(f"Kinesis polling error: {str(e)}")
            retries += 1
            if retries == MAX_RETRIES:
                logger.error(f"Max retries exceeded for Kinesis polling: {str(e)}")
                return False
            wait_time = min(2**retries + random.uniform(0, 0.5), 30)
            logger.warning(
                f"Kinesis polling failed, retrying in {wait_time:.2f} seconds..."
            )
            time.sleep(wait_time)

        except Exception as e:
            logger.error(f"Unexpected error in polling loop: {str(e)}")
            time.sleep(POLL_INTERVAL_SECONDS)


def main():
    logger.info("Starting Kinesis consumer loop")
    while True:
        try:
            if not process_records():
                logger.warning("Process records failed, retrying after delay")
            time.sleep(POLL_INTERVAL_SECONDS)
        except Exception as e:
            logger.error(f"Fatal error in consumer loop: {str(e)}")
            time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
