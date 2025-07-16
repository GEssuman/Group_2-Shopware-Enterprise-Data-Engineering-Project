import pandas as pd
import os
import boto3
import logging
from dotenv import load_dotenv
from botocore.exceptions import BotoCoreError, ClientError
from io import StringIO
from datetime import datetime
# Load env variables from .env file
load_dotenv()
bucket_name = os.getenv("AWS_S3_BUCKET")


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]: %(message)s")
logger = logging.getLogger()
s3 = boto3.client('s3')

schema = {
    "required_columns": [
       "transaction_id","store_id", "product_id", "quantity", 
       "revenue", "discount_applied", "timestamp" 
    ],
    "not_nulls": ["transaction_id","store_id", "product_id", "quantity", 
       "revenue", "timestamp" ]
}

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

def download_from_s3(s3_uri):
    """ Download a CSV file from S3 and load it into a Pandas DataFrame """
    s3 = boto3.client("s3")
    try:
        bucket, key = s3_uri.replace("s3://", "").split("/", 1)
        logging.info(f"Downloading file from {s3_uri}")
        obj = s3.get_object(Bucket=bucket, Key=key)
        data = obj['Body'].read().decode('utf-8')
        return pd.read_csv(StringIO(data))
    except Exception as error:
        logger.error(f"Error reading file {key}: {error}")
        return None

def validate_file(df, filename):
    """Validate a DataFrame against the defined schema"""
    logger.info(f"Validating file: {filename}")

    # check required columns
    missing_cols = set(schema["required_columns"]) - set(df.columns)
    if missing_cols:
        reason = f"Missing required columns: {missing_cols}"
        logger.error(f"Validation failed for {filename}: {reason}")
        return False, reason

    # check for not-null columns
    for col in schema["not_nulls"]:
        if df[col].isnull().any():
            reason = f"Column '{col}' contains null values"
            logger.error(f"Validation failed for {filename}: {reason}")
            return False, reason
        
    logger.info(f"File '{filename}' passed validation")
    return True, None

def quarantine_file(bucket, original_key ,reason):
    """
    Move the invalid file to a 'quarantine/' folder in the same S3 bucket.
    """
    try:
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        filename = os.path.basename(original_key)
        quarantine_key = f"quarantine/{timestamp}_{filename}"

        logger.warning(f"Quarantining file: {original_key} to {quarantine_key} | Reason: {reason}")
        
        # Copy file to quarantine location
        s3.copy_object(
            Bucket=bucket,
            CopySource={'Bucket': bucket, 'Key': original_key},
            Key=quarantine_key
        )

        # Optionally delete original (to simulate 'move')
        s3.delete_object(Bucket=bucket, Key=original_key)
        logger.info(f"File {original_key} moved to quarantine")

    except Exception as e:
        logger.error(f"Failed to quarantine file {original_key}: {e}")

def main():
    if not bucket_name:
        logger.error("AWS_S3_BUCKET environment variable is not set.")
        return
    
    files = list_files(bucket_name, "POS")

    if len(files) == 0:
        logger.info(f"No files available in the bucket to validate")
        return
    
    for file in files:

        df = download_from_s3(f"s3://{bucket_name}/{file}")
        if df is None:
            quarantine_file(bucket_name, file, "Failed to load file as DataFrame")
            continue

        is_valid, reason = validate_file(df, file)
        if not is_valid:
            quarantine_file(bucket_name, file, reason)
    

    


if "__main__" == __name__:
    main()