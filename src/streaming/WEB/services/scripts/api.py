import os
import json
import time
import logging
import boto3
import requests
from botocore.exceptions import BotoCoreError, ClientError
from requests.exceptions import RequestException
from datetime import datetime


# Configuration from environment or defaults
API_URL = os.getenv("API_URL")
KINESIS_STREAM_NAME = os.getenv("KINESIS_STREAM_NAME")
ERROR_S3_BUCKET = os.getenv("ERROR_S3_BUCKET")
VALID_DATA_S3_BUCKET = os.getenv("VALID_DATA_S3_BUCKET")  # New bucket for valid data
FAILED_RECORDS_SNS_ARN = os.getenv("FAILED_RECORDS_SNS_ARN")
REGION = os.getenv("REGION") or os.getenv("AWS_REGION")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", 1))
MAX_RETRIES_ON_KINESIS = 3


# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


# Validate required env vars early
required_env_vars = ["API_URL", "KINESIS_STREAM_NAME"]
missing_vars = [var for var in required_env_vars if not os.getenv(var)]
if missing_vars:
    logger.error(f"Missing required environment variables: {missing_vars}")
    exit(1)


# AWS clients
kinesis = boto3.client("kinesis", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)
sns = boto3.client("sns", region_name=REGION)


EXPECTED_SCHEMA = {
    "session_id": str,
    "user_id": (int, type(None)),
    "page": str,
    "device_type": (str, type(None)),
    "browser": (str, type(None)),
    "event_type": (str, type(None)),
    "timestamp": (float, int)
}


def validate_record(record):
    for field, expected_type in EXPECTED_SCHEMA.items():
        # Required fields check
        if field not in record:
            logger.warning(f"Missing required field '{field}'")
            return False
        value = record[field]
        if isinstance(expected_type, tuple):
            if value is not None and not isinstance(value, expected_type):
                logger.warning(f"Field '{field}' has wrong type. Got {type(value)}, expected {expected_type}")
                return False
        elif not isinstance(value, expected_type):
            logger.warning(f"Field '{field}' has wrong type. Got {type(value)}, expected {expected_type}")
            return False
    return True


def upload_to_s3(data):
    if not ERROR_S3_BUCKET:
        logger.error("Missing ERROR_S3_BUCKET. Skipping S3 upload.")
        return
    try:
        timestamp_str = datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%S-%fZ")
        key = f"failed_records/{timestamp_str}.json"
        s3.put_object(
            Bucket=ERROR_S3_BUCKET,
            Key=key,
            Body=json.dumps(data, ensure_ascii=False).encode('utf-8'),
            ContentType="application/json"
        )
        logger.info(f"Uploaded invalid record to s3://{ERROR_S3_BUCKET}/{key}")

        if FAILED_RECORDS_SNS_ARN:
            sns.publish(
                TopicArn=FAILED_RECORDS_SNS_ARN,
                Message=json.dumps({
                    "message": "Validation failure",
                    "s3_bucket": ERROR_S3_BUCKET,
                    "s3_key": key,
                    "record": data
                }),
                Subject="Validation Failed"
            )
            logger.info(f"SNS notification sent to {FAILED_RECORDS_SNS_ARN}")
    except (BotoCoreError, ClientError) as e:
        logger.error(f"Failed to upload or notify: {e}")


def upload_valid_record_to_s3(data):
    if not VALID_DATA_S3_BUCKET:
        logger.warning("VALID_DATA_S3_BUCKET not set, skipping upload of valid record to S3.")
        return
    try:
        timestamp_str = datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%S-%fZ")
        key = f"valid_records/{timestamp_str}.json"
        s3.put_object(
            Bucket=VALID_DATA_S3_BUCKET,
            Key=key,
            Body=json.dumps(data, ensure_ascii=False).encode('utf-8'),
            ContentType="application/json"
        )
        logger.info(f"Uploaded valid record to s3://{VALID_DATA_S3_BUCKET}/{key}")
    except (BotoCoreError, ClientError) as e:
        logger.error(f"Failed to upload valid record to S3: {e}")


def stream_to_kinesis(data):
    partition_key = data.get("session_id") or f"fallback-{int(time.time())}"
    for attempt in range(1, MAX_RETRIES_ON_KINESIS + 1):
        try:
            kinesis.put_record(
                StreamName=KINESIS_STREAM_NAME,
                Data=json.dumps(data, ensure_ascii=False),
                PartitionKey=partition_key,
            )
            logger.info("Successfully sent record to Kinesis")
            return True
        except Exception as e:
            logger.error(f"Kinesis put_record failed on attempt {attempt}: {e}")
            time.sleep(2 ** attempt)
    logger.error("Giving up on sending record to Kinesis after retries.")
    return False


def process_event(event):
    if validate_record(event):
        upload_valid_record_to_s3(event)
        if not stream_to_kinesis(event):
            upload_to_s3(event)  # fallback to error bucket if Kinesis fails
    else:
        upload_to_s3(event)  # upload invalid record to error bucket


def poll_api():
    while True:
        try:
            logger.info("Polling API...")
            response = requests.get(API_URL, timeout=10)
            response.raise_for_status()
            data = response.json()

            # Support single event or list of events
            if isinstance(data, list):
                for event in data:
                    process_event(event)
            else:
                process_event(data)

        except RequestException as e:
            logger.error(f"API request failed: {e}")
        except Exception as e:
            logger.exception(f"Unexpected error: {e}")

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    poll_api()
