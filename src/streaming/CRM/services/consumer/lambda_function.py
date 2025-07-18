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
