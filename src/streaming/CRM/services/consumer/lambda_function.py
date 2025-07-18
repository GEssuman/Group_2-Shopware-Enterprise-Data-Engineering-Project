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
ATHENA_DATABASE = os.environ["ATHENA_TABLE"]
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
