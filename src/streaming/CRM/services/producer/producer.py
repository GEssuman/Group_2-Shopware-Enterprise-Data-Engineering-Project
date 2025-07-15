import boto3
import requests
import json
import time
import random
import logging
import uuid
from datetime import datetime
from botocore.exceptions import ClientError
from requests.exceptions import RequestException, HTTPError, ConnectionError, Timeout

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# AWS Clients
kinesis = boto3.client("kinesis")
s3 = boto3.client("s3")

# Configuration
API_ENDPOINT = "http://3.248.199.26:8000/api/customer-interaction/"
STREAM_NAME = "ApiDataStream"
BUCKET = "my-api-data-2025"
MAX_RETRIES = 5
TIMEOUT_SECONDS = 10
MAX_BACKOFF_SECONDS = 30
POLL_INTERVAL_SECONDS = 10

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


def validate_record(record):
    """Validate a single record against the schema and non-null requirements."""
    try:
        for field in REQUIRED_FIELDS:
            if field not in record or record[field] is None:
                return False, f"Missing or null field: {field}"

        for field, expected_type in EXPECTED_SCHEMA.items():
            if field in record and not isinstance(record[field], expected_type):
                return (
                    False,
                    f"Invalid type for {field}: expected {expected_type}, got {type(record[field])}",
                )

        return True, None
    except Exception as e:
        return False, f"Validation error: {str(e)}"


def save_invalid_record(record, error):
    """Save invalid record to S3."""
    try:
        timestamp = datetime.utcnow()
        s3_key = (
            f"invalid/year={timestamp.year}/month={timestamp.month:02d}/"
            f"day={timestamp.day:02d}/{timestamp.isoformat()}_{uuid.uuid4()}.json"
        )
        s3.put_object(
            Bucket=BUCKET,
            Key=s3_key,
            Body=json.dumps(
                {"error": error, "record": record, "timestamp": timestamp.isoformat()}
            ),
        )
        logger.info(f"Saved invalid record to S3: {s3_key}")
    except ClientError as e:
        logger.error(f"Failed to save invalid record to S3: {str(e)}")


def poll_api():
    retries = 0
    while retries < MAX_RETRIES:
        try:
            response = requests.get(API_ENDPOINT, timeout=TIMEOUT_SECONDS)
            response.raise_for_status()

            try:
                record = response.json()
            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON response: {str(e)}")
                save_invalid_record({}, str(e))
                return False

            if isinstance(record, dict):
                records = [record]
            elif isinstance(record, list):
                records = record
            else:
                logger.error(f"Unexpected response format: {type(record)}")
                save_invalid_record(
                    record, f"Unexpected response format: {type(record)}"
                )
                return False

            kinesis_records = []
            for r in records:
                is_valid, error = validate_record(r)
                if is_valid:
                    kinesis_records.append(
                        {
                            "Data": json.dumps(r),
                            "PartitionKey": str(r.get("customer_id", "default")),
                        }
                    )
                else:
                    logger.warning(f"Invalid record: {error}, Record: {r}")
                    save_invalid_record(r, error)

            if kinesis_records:
                try:
                    kinesis_response = kinesis.put_records(
                        StreamName=STREAM_NAME, Records=kinesis_records
                    )
                    failed_count = kinesis_response.get("FailedRecordCount", 0)
                    if failed_count > 0:
                        logger.error(
                            f"Failed to send {failed_count} records to Kinesis: {kinesis_response}"
                        )
                        raise ClientError(
                            {"Error": {"Code": "PutRecordsFailed"}}, "put_records"
                        )
                    logger.info(
                        f"Successfully sent {len(kinesis_records)} records to Kinesis"
                    )
                    return True
                except ClientError as e:
                    logger.error(f"Kinesis error: {str(e)}")
                    retries += 1
                    wait_time = min(
                        2**retries + random.uniform(0, 0.5), MAX_BACKOFF_SECONDS
                    )
                    logger.info(f"Backing off for {wait_time:.2f} seconds...")
                    time.sleep(wait_time)
            else:
                logger.info("No valid records to send to Kinesis")
                return True

        except Timeout:
            logger.error("API request timed out")
            retries += 1
            wait_time = min(2**retries + random.uniform(0, 0.5), MAX_BACKOFF_SECONDS)
            logger.info(f"Backing off for {wait_time:.2f} seconds...")
            time.sleep(wait_time)

        except ConnectionError:
            logger.error("Network connection error")
            retries += 1
            wait_time = min(2**retries + random.uniform(0, 0.5), MAX_BACKOFF_SECONDS)
            logger.info(f"Backing off for {wait_time:.2f} seconds...")
            time.sleep(wait_time)

        except HTTPError as e:
            status_code = e.response.status_code if e.response else "Unknown"
            if status_code == 429:
                wait_time = min(
                    2**retries + random.uniform(0, 0.5), MAX_BACKOFF_SECONDS
                )
                logger.warning(
                    f"Rate limited (429). Backing off for {wait_time:.2f} seconds..."
                )
                time.sleep(wait_time)
                retries += 1
            else:
                logger.error(f"HTTP error: {str(e)}, Status: {status_code}")
                save_invalid_record({}, f"HTTP error: {str(e)}, Status: {status_code}")
                return False

    logger.error("Max retries exceeded for transient errors in poll attempt")
    return False


def main():
    logger.info("Starting API polling loop")
    while True:
        try:
            success = poll_api()
            if not success:
                logger.warning("Poll attempt failed, continuing to next poll")
        except Exception as e:
            logger.error(f"Fatal error in polling loop: {str(e)}")
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
