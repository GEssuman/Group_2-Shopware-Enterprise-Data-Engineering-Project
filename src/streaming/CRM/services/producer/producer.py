import boto3
import json
import logging
import time
import requests
from botocore.exceptions import ClientError
from dotenv import load_dotenv
import os
import random
import uuid
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Configuration
REQUIRED_ENV_VARS = ["STREAM_NAME", "BUCKET", "API_URL"]
POLL_INTERVAL_SECONDS = 0.5
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
API_URL = os.getenv("API_URL")

# AWS Clients
kinesis = boto3.client("kinesis")
s3 = boto3.client("s3")

# Required fields only
REQUIRED_FIELDS = ["customer_id", "interaction_type", "timestamp"]


def validate_record(data):
    """Validate that required fields are present and non-null only."""
    try:
        for field in REQUIRED_FIELDS:
            if field not in data or data[field] is None:
                return False, f"Missing or null field: {field}"
        return True, None
    except Exception as e:
        return False, f"Validation error: {str(e)}"


def save_invalid_record(record, error):
    """Save invalid record to S3."""
    retries = 0
    s3_key = f"invalid/year={datetime.utcnow().year}/month={datetime.utcnow().month:02d}/day={datetime.utcnow().day:02d}/{uuid.uuid4()}.json"
    while retries < MAX_RETRIES:
        try:
            s3.put_object(
                Bucket=BUCKET,
                Key=s3_key,
                Body=json.dumps(
                    {
                        "error": error,
                        "record": record,
                        "timestamp": datetime.utcnow().isoformat(),
                    }
                ),
            )
            logger.info(f"Saved invalid record to S3: {s3_key}")
            return True
        except ClientError as e:
            retries += 1
            if retries == MAX_RETRIES:
                logger.error(
                    f"Failed to save invalid record to S3 after {MAX_RETRIES} retries: {str(e)}"
                )
                return False
            wait_time = min(2**retries + random.uniform(0, 0.5), 30)
            logger.warning(f"S3 write failed, retrying in {wait_time:.2f} seconds...")
            time.sleep(wait_time)
    return False


def poll_api():
    """Poll the API and send valid records to Kinesis."""
    retries = 0
    while retries < MAX_RETRIES:
        try:
            response = requests.get(API_URL)
            response.raise_for_status()
            records = response.json()
            logger.debug(f"Raw API response: {records}")

            kinesis_records = []
            for r in records:
                is_valid, error = validate_record(r)
                if is_valid:
                    try:
                        record_data = json.dumps(r)
                        logger.debug(f"Sending record to Kinesis: {record_data}")
                        kinesis_records.append(
                            {
                                "Data": record_data,
                                "PartitionKey": str(r.get("customer_id", "default")),
                            }
                        )
                    except (TypeError, ValueError) as e:
                        logger.error(f"Failed to encode record: {str(e)}, Record: {r}")
                        save_invalid_record(r, f"Encoding error: {str(e)}")
                else:
                    logger.warning(f"Invalid record: {error}, Record: {r}")
                    save_invalid_record(r, error)

            if kinesis_records:
                retries_kinesis = 0
                while retries_kinesis < MAX_RETRIES:
                    try:
                        kinesis.put_records(
                            StreamName=STREAM_NAME, Records=kinesis_records
                        )
                        logger.info(
                            f"Successfully sent {len(kinesis_records)} records to Kinesis"
                        )
                        return True
                    except ClientError as e:
                        retries_kinesis += 1
                        if retries_kinesis == MAX_RETRIES:
                            logger.error(
                                f"Failed to send records to Kinesis after {MAX_RETRIES} retries: {str(e)}"
                            )
                            return False
                        wait_time = min(2**retries_kinesis + random.uniform(0, 0.5), 30)
                        logger.warning(
                            f"Kinesis write failed, retrying in {wait_time:.2f} seconds..."
                        )
                        time.sleep(wait_time)
            else:
                logger.info("No valid records to send to Kinesis")
            return True

        except requests.RequestException as e:
            retries += 1
            if retries == MAX_RETRIES:
                logger.error(
                    f"Failed to fetch API after {MAX_RETRIES} retries: {str(e)}"
                )
                return False
            wait_time = min(2**retries + random.uniform(0, 0.5), 30)
            logger.warning(
                f"API request failed, retrying in {wait_time:.2f} seconds..."
            )
            time.sleep(wait_time)


def main():
    logger.info("Starting API polling loop")
    while True:
        try:
            if not poll_api():
                logger.warning("API polling failed, retrying after delay")
            time.sleep(POLL_INTERVAL_SECONDS)
        except Exception as e:
            logger.error(f"Fatal error in polling loop: {str(e)}")
            time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
