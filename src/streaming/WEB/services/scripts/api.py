import os
import json
import time
import logging
import boto3
import requests
from botocore.exceptions import BotoCoreError, ClientError
from requests.exceptions import HTTPError, RequestException
from datetime import datetime

# Configuration
API_URL = os.getenv("API_URL")
KINESIS_STREAM_NAME = os.getenv("KINESIS_STREAM_NAME")
S3_BUCKET = os.getenv("S3_BUCKET")
EXTRA_COLUMNS_S3_FOLDER = "extra_columns"
FAILED_RECORDS_SNS_ARN = os.getenv("FAILED_RECORDS_SNS_ARN")
REGION = os.getenv("REGION") or os.getenv("AWS_REGION")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", 1))  # Default to 1s if unset
MAX_RETRIES_ON_KINESIS = 3

# Batch config (adjust as needed)
BATCH_SIZE = int(os.getenv("S3_BATCH_SIZE", 100))        # write to S3 every 100 events
BATCH_SECONDS = int(os.getenv("S3_BATCH_SECONDS", 10))   # or every 10 seconds, whichever comes first

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# AWS Clients
kinesis = boto3.client("kinesis", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)
sns = boto3.client("sns", region_name=REGION)

# Expected schema
EXPECTED_FIELDS = ["session_id", "user_id", "page", "device_type", "browser", "event_type", "timestamp"]

# Validate env vars
required_env_vars = ["API_URL", "KINESIS_STREAM_NAME", "S3_BUCKET"]
missing = [v for v in required_env_vars if not os.getenv(v)]
if missing:
    logger.error(f"Missing required environment variables: {missing}")
    exit(1)

def has_required_ids(event):
    return event.get("session_id") and event.get("user_id") is not None

def upload_ndjson_to_s3(bucket, prefix, events):
    if not events:
        return None
    try:
        timestamp_str = datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%S-%fZ")
        key = f"{prefix}/{timestamp_str}.ndjson"
        body = '\n'.join(json.dumps(e, ensure_ascii=False) for e in events)
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=body.encode('utf-8'),
            ContentType="application/x-ndjson"
        )
        logger.info(f"Uploaded batch ({len(events)}) records to s3://{bucket}/{key}")
        return key
    except (BotoCoreError, ClientError) as e:
        logger.error(f"S3 batch upload failed: {e}")
        return None

def upload_to_s3(bucket, prefix, data):
    # For single failed records (e.g., failed validation)
    try:
        timestamp_str = datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%S-%fZ")
        key = f"{prefix}/{timestamp_str}.json"
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(data, ensure_ascii=False).encode('utf-8'),
            ContentType="application/json"
        )
        logger.info(f"Uploaded record to s3://{bucket}/{key}")
        return key
    except (BotoCoreError, ClientError) as e:
        logger.error(f"S3 upload failed: {e}")
        return None

def notify_failure(data, s3_key):
    if not FAILED_RECORDS_SNS_ARN:
        return
    try:
        sns.publish(
            TopicArn=FAILED_RECORDS_SNS_ARN,
            Message=json.dumps({
                "message": "Validation failure",
                "s3_bucket": S3_BUCKET,
                "s3_key": s3_key,
                "record": data
            }),
            Subject="Validation Failed"
        )
        logger.info(f"SNS notification sent for failed record")
    except Exception as e:
        logger.error(f"SNS publish failed: {e}")

def stream_to_kinesis(data):
    partition_key = data.get("session_id") or f"fallback-{int(time.time())}"
    for attempt in range(1, MAX_RETRIES_ON_KINESIS + 1):
        try:
            kinesis.put_record(
                StreamName=KINESIS_STREAM_NAME,
                Data=json.dumps(data, ensure_ascii=False),
                PartitionKey=partition_key
            )
            logger.info("Successfully sent record to Kinesis")
            return True
        except Exception as e:
            logger.error(f"Kinesis put_record failed on attempt {attempt}: {e}")
            time.sleep(2 ** attempt)
    logger.error("Giving up on sending record to Kinesis after retries.")
    return False

def process_event(event, valid_batch, extra_batch):
    if not has_required_ids(event):
        key = upload_to_s3(S3_BUCKET, "failed_records", event)
        if key:
            notify_failure(event, key)
        return

    extra_keys = [k for k in event.keys() if k not in EXPECTED_FIELDS]
    cleaned_event = {k: event[k] for k in EXPECTED_FIELDS if k in event}

    if extra_keys:
        # Only upload to extra_columns batch, not valid_records (don't duplicate)
        extended = dict(cleaned_event)  
        for k in extra_keys:
            extended[k] = event[k]
        extra_batch.append(extended)
    else:
        valid_batch.append(cleaned_event)

    # Always stream to Kinesis if valid
    if not stream_to_kinesis(cleaned_event):
        upload_to_s3(S3_BUCKET, "kinesis_failures", cleaned_event)

def poll_api():
    valid_batch = []
    extra_batch = []
    last_s3_write = time.time()

    while True:
        try:
            logger.info("Polling API...")
            response = requests.get(API_URL, timeout=10)
            try:
                response.raise_for_status()
            except HTTPError as e:
                if response.status_code == 404:
                    logger.warning(f"API returned 404: {API_URL}")
                else:
                    logger.error(f"API HTTP error: {e}")
                time.sleep(POLL_INTERVAL)
                continue

            data = response.json()
            now = time.time()
            if isinstance(data, list):
                for event in data:
                    process_event(event, valid_batch, extra_batch)
            else:
                process_event(data, valid_batch, extra_batch)

            # Check if we should flush batches
            flush_due = (
                len(valid_batch) >= BATCH_SIZE or
                len(extra_batch) >= BATCH_SIZE or
                (now - last_s3_write) >= BATCH_SECONDS
            )
            if flush_due:
                if valid_batch:
                    upload_ndjson_to_s3(S3_BUCKET, "valid_records", valid_batch)
                    valid_batch.clear()
                if extra_batch:
                    upload_ndjson_to_s3(S3_BUCKET, EXTRA_COLUMNS_S3_FOLDER, extra_batch)
                    extra_batch.clear()
                last_s3_write = now

        except RequestException as e:
            logger.error(f"API request failed: {e}")
        except Exception as e:
            logger.exception(f"Unexpected error: {e}")
        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    poll_api()
