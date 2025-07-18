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
