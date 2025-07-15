import os
import json
import time
import logging
import requests
import boto3
from botocore.exceptions import BotoCoreError, ClientError
from requests.exceptions import RequestException
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables from .env file
load_dotenv()

# Configuration from environment or defaults
API_URL = os.getenv("API_URL")
KINESIS_STREAM_NAME = os.getenv("KINESIS_STREAM_NAME")
ERROR_S3_BUCKET = os.getenv("ERROR_S3_BUCKET")  # S3 bucket for error records
FAILED_RECORDS_SNS_ARN = os.getenv("FAILED_RECORDS_SNS_ARN")  # SNS Topic ARN for notifications
REGION = os.getenv("AWS_REGION")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", 10))
MAX_RETRIES = 5

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)  # Create a logger instance with the module name


# Initialize AWS clients
kinesis = boto3.client("kinesis", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)
sns = boto3.client("sns", region_name=REGION)  # SNS client

# Define expected schema with types (use Python types for quick checks)
EXPECTED_SCHEMA = {
    "session_id": str,
    "user_id": (int, type(None)),    
    "page": str,
    "device_type": (str, type(None)), 
    "browser": (str, type(None)),     
    "event_type": (str, type(None)), 
    "timestamp": (float, int)         # Expect float or int epoch time
}

def validate_record(record):
    """
    Validates a single record against EXPECTED_SCHEMA.
    Returns: (bool) True if valid, False if not
    """
    for field, expected_type in EXPECTED_SCHEMA.items():
        if field not in record:
            logging.warning(f"Validation failed: Missing required field '{field}'.")
            return False
        value = record[field]
        # Check nullable: if expected type is tuple including NoneType
        if isinstance(expected_type, tuple):
            if value is not None and not isinstance(value, expected_type):
                logging.warning(f"Validation failed: Field '{field}' has incorrect type. Expected {expected_type}, got {type(value)}.")
                return False
        else:
            if not isinstance(value, expected_type):
                logging.warning(f"Validation failed: Field '{field}' has incorrect type. Expected {expected_type}, got {type(value)}.")
                return False
    return True

def upload_to_s3(data):
    """
    Uploads invalid data JSON record to S3 for later analysis.
    Creates a uniquely named object key using timestamp.
    Also triggers SNS notification about the failure.
    """
    if not ERROR_S3_BUCKET:
        logging.error("ERROR_S3_BUCKET environment variable is not set. Cannot upload invalid records.")
        return

    try:
        timestamp_str = datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%S-%fZ")
        key = f"failed_records/{timestamp_str}.json"
        s3.put_object(
            Bucket=ERROR_S3_BUCKET,
            Key=key,
            Body=json.dumps(data).encode('utf-8'),
            ContentType="application/json"
        )
        logging.info(f"Uploaded invalid record to s3://{ERROR_S3_BUCKET}/{key}")

        if FAILED_RECORDS_SNS_ARN:
            message = {
                "message": "Validation failure detected",
                "s3_bucket": ERROR_S3_BUCKET,
                "s3_key": key,
                "record": data
            }
            sns.publish(
                TopicArn=FAILED_RECORDS_SNS_ARN,
                Message=json.dumps(message),
                Subject="Validation Failed Record Notification"
            )
            logging.info(f"Published SNS notification to {FAILED_RECORDS_SNS_ARN}")
        else:
            logging.warning("FAILED_RECORDS_SNS_ARN environment variable not set. No SNS notification sent.")

    except (BotoCoreError, ClientError) as e:
        logging.error(f"Failed to upload invalid record to S3 or publish SNS: {e}")


