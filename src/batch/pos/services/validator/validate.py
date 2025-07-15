import pandas as pd
import os
import boto3
import logging
from dotenv import load_dotenv
from botocore.exceptions import BotoCoreError, ClientError
# Load env variables from .env file
load_dotenv()
bucket_name = os.getenv("AWS_S3_BUCKET")


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]: %(message)s")
logger = logging.getLogger()
s3 = boto3.client('s3')

def list_files(bucket, prefix=""):
    """Return a list of files for a given prefix in an S3 bucket."""
    try:
        logger.info(f"Listing files in bucket: {bucket}, prefix: {prefix}")
        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)

        if "Contents" in response:
            csv_file = [obj["Key"] for obj in response["Contents"] if obj["Key"].endswith(".csv")]
            logger.info(f"Found {len(csv_file)} .csv file(s)")
            return csv_file
        else:
                logger.warning(f"No files found in bucket '{bucket}' with prefix '{prefix}'")
                return []
    except (BotoCoreError, ClientError) as e:
        logger.error(f"Error listing files from S3: {e}")
        return []

def validate_file():
     pass
def main(event, context):
    file_available = list_files(bucket_name, "POS")

    if len(file_available) == 0:
        logger.info(f"No files available in the bucket to validate")
        return
    


if "__main__" == __name__:
    main(event={}, context={})