import pandas as pd
import os
import boto3
import logging
from botocore.exceptions import BotoCoreError, ClientError
from io import StringIO
from datetime import datetime
import json
import sys
# Load env variables from .env file


raw_bucket_name = os.environ.get("AWS_RAW_S3_BUCKET") or sys.argv[sys.argv.index('--AWS_RAW_S3_BUCKET') + 1]
quarantine_bucket_name = os.environ.get("AWS_QUARANTINE_S3_BUCKET") or sys.argv[sys.argv.index('--AWS_QUARANTINE_S3_BUCKET') + 1]

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
        error_msg = f"Error listing files from S3: {e}"
        logger.error(error_msg)
        raise RuntimeError(error_msg)  # Let `main()` handle this

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
        quarantine_key = f"pos/{timestamp}_{filename}"

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

    summary = {
    "processed_files": 0,
    "quarantined_files": [],
    "errors": []
    }
    
    if not raw_bucket_name:
        msg = "AWS_S3_BUCKET environment variable is not set."
        logger.error(msg)
        summary["errors"].append(msg)
        return summary

    
    try:
        files = list_files(raw_bucket_name, "pos")
    except Exception as e:
        summary["errors"].append(str(e))
        return summary

    if len(files) == 0:
        logger.info("No files available in the bucket to validate")
        return summary
    
    for file in files:
        try:
            df = download_from_s3(f"s3://{raw_bucket_name}/{file}")
            if df is None:
                reason = "Failed to load file as DataFrame"
                quarantine_file(quarantine_bucket_name, file, reason)
                summary["quarantined_files"].append({"file": file, "reason": reason})
                continue

            is_valid, reason = validate_file(df, file)
            if not is_valid:
                quarantine_file(quarantine_bucket_name, file, reason)
                summary["quarantined_files"].append({"file": file, "reason": reason})
            else:
                summary["processed_files"] += 1
        except Exception as e:
            error_msg = f"Unexpected error while processing {file}: {str(e)}"
            logger.error(error_msg)
            summary["errors"].append(error_msg)


    return summary
    


if __name__ == "__main__":
    result = main()

    # Log to Glue console
    logger.info(f"Job Summary: {json.dumps(result)}")
    print(json.dumps(result))