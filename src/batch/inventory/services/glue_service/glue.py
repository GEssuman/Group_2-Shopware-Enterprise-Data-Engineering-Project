import sys
import boto3
import json
import logging
from datetime import datetime
from botocore.exceptions import ClientError
from typing import List, Tuple, Dict
import os
import time
import pandas as pd
import io

# -----------------------------------------------------------------------------
# 1. Configuration and Constants
# -----------------------------------------------------------------------------
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

PROJECT_BUCKET = "misc-gtp-proj"

S3_INV_RAW_PATH = f"s3://{PROJECT_BUCKET}/inventory_landing_zone/raw"
S3_INV_VALIDATED_PATH = f"s3://{PROJECT_BUCKET}/inventory_landing_zone/validated"
S3_INV_REJECTED_PATH = f"s3://{PROJECT_BUCKET}/inventory_landing_zone/rejected"
S3_LOG_PATH = f"s3://{PROJECT_BUCKET}/logs/glue/"

FILE_SIZE_THRESHOLD = 104857600  # 100MB
CHUNK_SIZE = 52428800            # 50MB
MAX_RETRY_ATTEMPTS = 3

REQUIRED_COLUMNS = {"inventory_id", "product_id", "warehouse_id", "stock_level", "last_updated"}
EXPECTED_DTYPES = {
    "inventory_id": "int64",
    "product_id": "int64",
    "warehouse_id": "int64",
    "stock_level": "int64",
    "restock_threshold": "Int64",   # Nullable integer
    "last_updated": "float64"
}

# Initialize boto3 client
s3_client = boto3.client(
    's3',
    config=boto3.session.Config(
        retries={
            'max_attempts': MAX_RETRY_ATTEMPTS,
            'mode': 'adaptive'
        }
    )
)

# -----------------------------------------------------------------------------
# 2. Helper Functions
# -----------------------------------------------------------------------------
def retry_operation(operation, max_attempts: int = MAX_RETRY_ATTEMPTS, delay: float = 1.0):
    def wrapper(*args, **kwargs):
        last_exception = None
        for attempt in range(max_attempts):
            try:
                return operation(*args, **kwargs)
            except Exception as e:
                last_exception = e
                if attempt < max_attempts - 1:
                    sleep_time = delay * (2 ** attempt)
                    logger.warning(f"Attempt {attempt + 1} failed: {e}. Retrying in {sleep_time}s...")
                    time.sleep(sleep_time)
                else:
                    logger.error(f"All {max_attempts} attempts failed for operation")
        raise last_exception
    return wrapper

def write_logs_to_s3(log_messages: List[str]) -> None:
    if not log_messages:
        return

    @retry_operation
    def _write_logs():
        try:
            timestamp = datetime.utcnow().strftime('%Y-%m-%d_%H')
            log_key = f"{S3_LOG_PATH.replace(f's3://{PROJECT_BUCKET}/', '')}validation_log_{timestamp}.log"
            log_content = "\n".join(log_messages) + "\n"
            existing_content = ""
            try:
                response = s3_client.get_object(Bucket=PROJECT_BUCKET, Key=log_key)
                existing_content = response['Body'].read().decode('utf-8')
            except ClientError as e:
                if e.response['Error']['Code'] != 'NoSuchKey':
                    raise
            s3_client.put_object(
                Bucket=PROJECT_BUCKET,
                Key=log_key,
                Body=existing_content + log_content,
                ServerSideEncryption='AES256',
                ContentType='text/plain'
            )
            logger.info(f"Successfully wrote {len(log_messages)} log entries to S3")
        except ClientError as e:
            logger.error(f"Failed to write logs to S3: {e}")
            logger.error("--- S3 Log Fallback ---")
            for msg in log_messages:
                logger.info(msg)
    _write_logs()

def get_files_to_process(incoming_path: str) -> List[Dict]:
    """
    Lists all objects in the incoming S3 path and returns file information.
    """
    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        incoming_bucket, incoming_prefix = incoming_path.replace("s3://", "").split('/', 1)
        files = []
        for page in paginator.paginate(Bucket=incoming_bucket, Prefix=incoming_prefix):
            for obj in page.get('Contents', []):
                key = obj['Key']
                if not key.endswith('/'):
                    file_info = {
                        'key': key,
                        'size': obj['Size'],
                        'last_modified': obj['LastModified'],
                        'needs_chunking': obj['Size'] > FILE_SIZE_THRESHOLD
                    }
                    files.append(file_info)
        logger.info(f"Found {len(files)} files to process")
        return files
    except Exception as e:
        logger.error(f"Error listing files from {incoming_path}: {e}")
        raise

# -----------------------------------------------------------------------------
# 3. Data Processing Functions
# -----------------------------------------------------------------------------
def read_jsonl_from_s3(file_key: str) -> pd.DataFrame:
    try:
        response = s3_client.get_object(Bucket=PROJECT_BUCKET, Key=file_key)
        content = response['Body'].read().decode('utf-8').strip()
        records = []
        # If file starts with '[', treat as a single JSON array
        if content.startswith('['):
            try:
                array = json.loads(content)
                if isinstance(array, list):
                    records.extend(array)
                else:
                    logger.error(f"Top-level JSON is not an array in {file_key}")
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse JSON array: {e}")
                raise
        else:
            # Parse as JSONL
            for line_num, line in enumerate(content.split('\n'), 1):
                if line.strip():
                    try:
                        record = json.loads(line)
                        records.append(record)
                    except json.JSONDecodeError as e:
                        logger.error(f"Invalid JSON at line {line_num}: {e}")
                        raise
        if not records:
            return pd.DataFrame()
        df = pd.DataFrame(records)
        logger.info(f"Successfully read {len(df)} records from {file_key}")
        return df
    except Exception as e:
        logger.error(f"Failed to read {file_key}: {e}")
        raise


def read_chunked_jsonl_from_s3(file_key: str, file_size: int) -> pd.DataFrame:
    logger.info(f"Reading large file {file_key} in chunks")
    all_records = []
    chunk_num = 0
    for start_byte in range(0, file_size, CHUNK_SIZE):
        end_byte = min(start_byte + CHUNK_SIZE - 1, file_size - 1)
        try:
            byte_range = f"bytes={start_byte}-{end_byte}"
            response = s3_client.get_object(
                Bucket=PROJECT_BUCKET,
                Key=file_key,
                Range=byte_range
            )
            chunk_content = response['Body'].read().decode('utf-8')
            if chunk_num > 0:
                lines = chunk_content.split('\n')[1:]  # skip incomplete first line
            else:
                lines = chunk_content.split('\n')
            if start_byte + CHUNK_SIZE < file_size:
                lines = lines[:-1]  # remove incomplete last line
            for line in lines:
                if line.strip():
                    try:
                        record = json.loads(line)
                        all_records.append(record)
                    except json.JSONDecodeError as e:
                        logger.error(f"Invalid JSON in chunk {chunk_num}: {e}")
                        raise
            chunk_num += 1
            logger.info(f"Processed chunk {chunk_num} ({len(lines)} lines)")
        except Exception as e:
            logger.error(f"Error reading chunk {chunk_num}: {e}")
            raise
    if not all_records:
        return pd.DataFrame()
    df = pd.DataFrame(all_records)
    logger.info(f"Successfully read {len(df)} records from chunked file {file_key}")
    return df

def validate_dataframe(df: pd.DataFrame) -> Tuple[bool, List[str]]:
    errors = []
    try:
        # Check if DataFrame is empty
        if df.empty:
            errors.append("DataFrame is empty")
            return False, errors
        # Check for missing required columns
        available_columns = set(df.columns)
        missing_cols = REQUIRED_COLUMNS - available_columns
        if missing_cols:
            errors.append(f"Missing required columns: {', '.join(sorted(missing_cols))}")
            return False, errors
        # Check for extra columns (log as warning)
        extra_cols = available_columns - set(EXPECTED_DTYPES.keys())
        if extra_cols:
            logger.warning(f"Extra columns ignored: {', '.join(sorted(extra_cols))}")
        validated_df = df.copy()
        for col_name, expected_dtype in EXPECTED_DTYPES.items():
            if col_name in validated_df.columns:
                try:
                    if expected_dtype == "Int64":  # Nullable integer
                        validated_df[col_name] = pd.to_numeric(validated_df[col_name], errors='coerce').astype("Int64")
                    else:
                        validated_df[col_name] = validated_df[col_name].astype(expected_dtype)
                except Exception as e:
                    errors.append(f"Failed to convert column {col_name} to {expected_dtype}: {e}")
                    return False, errors
        # Check for null values in required columns
        for col_name in REQUIRED_COLUMNS:
            if col_name in validated_df.columns:
                null_count = validated_df[col_name].isnull().sum()
                if null_count > 0:
                    errors.append(f"Found {null_count} null values in required column {col_name}")
                    return False, errors
        business_valid, business_errors = validate_business_rules_pandas(validated_df)
        if not business_valid:
            errors.extend(business_errors)
            return False, errors
        logger.info(f"Validation passed for {len(validated_df)} rows")
        return True, []
    except Exception as e:
        errors.append(f"Validation failed: {e}")
        logger.error(f"Validation error: {e}")
        return False, errors

def validate_business_rules_pandas(df: pd.DataFrame) -> Tuple[bool, List[str]]:
    errors = []
    try:
        # Rule 1: stock_level >= 0
        negative_stock = (df['stock_level'] < 0).sum()
        if negative_stock > 0:
            errors.append(f"Found {negative_stock} rows with negative stock levels")
        # Rule 2: restock_threshold >= 0 when not null
        if 'restock_threshold' in df.columns:
            invalid_threshold = ((df['restock_threshold'].notna()) & (df['restock_threshold'] < 0)).sum()
            if invalid_threshold > 0:
                errors.append(f"Found {invalid_threshold} rows with negative restock thresholds")
        # Rule 3: last_updated should not be >24h in future
        current_timestamp = datetime.utcnow().timestamp()
        future_threshold = current_timestamp + 86400
        future_updates = (df['last_updated'] > future_threshold).sum()
        if future_updates > 0:
            errors.append(f"Found {future_updates} rows with future last_updated timestamps")
        return len(errors) == 0, errors
    except Exception as e:
        errors.append(f"Business rule validation failed: {e}")
        return False, errors

def write_dataframe_to_s3_parquet(df: pd.DataFrame, output_path: str, file_key:str) -> None:
    """
    Write pandas DataFrame to S3 as Parquet format. File is named based on the original file.
    """
    try:
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, engine='pyarrow', index=False)
        parquet_buffer.seek(0)
        filename = os.path.splitext(os.path.basename(file_key))[0]
        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        s3_key = f"{output_path.replace(f's3://{PROJECT_BUCKET}/', '')}/{filename}_{timestamp}.parquet"
        s3_client.put_object(
            Bucket=PROJECT_BUCKET,
            Key=s3_key,
            Body=parquet_buffer.getvalue(),
            ServerSideEncryption='AES256',
            ContentType='application/octet-stream'
        )
        logger.info(f"Successfully wrote {len(df)} rows to s3://{PROJECT_BUCKET}/{s3_key}")
    except Exception as e:
        logger.error(f"Failed to write DataFrame to S3: {e}")
        raise

# -----------------------------------------------------------------------------
# 4. File Processing
# -----------------------------------------------------------------------------
def move_s3_object(source_key: str, dest_path: str) -> None:
    """
    Move an S3 object with error handling and retry.
    """
    @retry_operation
    def _move_object():
        try:
            file_name = os.path.basename(source_key)
            dest_bucket, dest_prefix = dest_path.replace("s3://", "").split('/', 1)
            # Add timestamp to avoid conflicts
            timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
            name_parts = file_name.rsplit('.', 1)
            if len(name_parts) == 2:
                dest_key = f"{dest_prefix.rstrip('/')}/{name_parts[0]}_{timestamp}.{name_parts[1]}"
            else:
                dest_key = f"{dest_prefix.rstrip('/')}/{file_name}_{timestamp}"
            # Copy object
            copy_source = {'Bucket': PROJECT_BUCKET, 'Key': source_key}
            s3_client.copy_object(
                CopySource=copy_source,
                Bucket=dest_bucket,
                Key=dest_key,
                ServerSideEncryption='AES256'
            )
            # Delete original
            #s3_client.delete_object(Bucket=PROJECT_BUCKET, Key=source_key)
            logger.info(f"Moved {source_key} to s3://{dest_bucket}/{dest_key}")
        except ClientError as e:
            logger.error(f"Failed to move {source_key}: {e}")
            raise
    _move_object()

def process_single_file(file_info: Dict) -> Tuple[bool, List[str], str]:
    file_key = file_info['key']
    logger.info(f"Processing file: {file_key} (Size: {file_info['size']} bytes)")
    try:
        # Read file based on size
        if file_info['needs_chunking']:
            df = read_chunked_jsonl_from_s3(file_key, file_info['size'])
        else:
            df = read_jsonl_from_s3(file_key)

        if df.empty:
            move_s3_object(file_key, S3_INV_REJECTED_PATH)
            return False, ["File is empty"], "EMPTY"

        # Validate data
        is_valid, errors = validate_dataframe(df)
        if is_valid:
            # Write to validated zone as parquet
            write_dataframe_to_s3_parquet(df, S3_INV_VALIDATED_PATH, file_key)
            # Move the original to validated zone as well
            # move_s3_object(file_key, S3_INV_VALIDATED_PATH)
            # Delete the original JSON file from the raw path after successful processing and writing Parquet
            # s3_client.delete_object(Bucket=PROJECT_BUCKET, Key=file_key)
            return True, [], "SUCCESS"
        else:
            # Move invalid file to rejected zone
            move_s3_object(file_key, S3_INV_REJECTED_PATH)
            return False, errors, "REJECTED"
    except Exception as e:
        logger.error(f"Critical error processing {file_key}: {e}")
        try:
            move_s3_object(file_key, S3_INV_REJECTED_PATH)
        except Exception:
            logger.error(f"Could not move file {file_key} to rejected zone")
        return False, [f"Processing failed: {str(e)}"], "ERROR"

# -----------------------------------------------------------------------------
# 5. Monitoring and Reporting Functions
# -----------------------------------------------------------------------------
def publish_metrics_to_cloudwatch(metrics: Dict):
    try:
        cloudwatch = boto3.client('cloudwatch')
        metric_data = []
        for metric_name, value in metrics.items():
            metric_data.append({
                'MetricName': metric_name,
                'Value': value,
                'Unit': 'Count',
                'Dimensions': [
                    {'Name': 'JobName', 'Value': 'inventory-validation-job'}
                ]
            })
        cloudwatch.put_metric_data(
            Namespace='GlueJobs/InventoryValidation',
            MetricData=metric_data
        )
        logger.info(f"Published {len(metrics)} metrics to CloudWatch")
    except Exception as e:
        logger.error(f"Failed to publish metrics to CloudWatch: {e}")

def create_processing_report(total_files: int, successful_files: int,
                            rejected_files: int, error_files: int,
                            duration: float) -> Dict:
    report = {
        'job_execution': {
            'start_time': datetime.utcnow().isoformat(),
            'duration_seconds': duration,
            'total_files': total_files,
            'successful_files': successful_files,
            'rejected_files': rejected_files,
            'error_files': error_files,
        }
    }
    return report

# -----------------------------------------------------------------------------
# 6. Main Execution
# -----------------------------------------------------------------------------
def main():
    start_time = time.time()
    files_to_process = get_files_to_process(S3_INV_RAW_PATH)
    total_files = len(files_to_process)
    success_count = 0
    reject_count = 0
    error_count = 0
    log_messages = []

    for file_info in files_to_process:
        success, errors, status = process_single_file(file_info)
        file_name = os.path.basename(file_info['key'])
        if success:
            msg = f"SUCCESS: {file_name}"
            success_count += 1
        elif status == "REJECTED":
            msg = f"REJECTED: {file_name} -- errors: {errors}"
            reject_count += 1
        else:
            msg = f"ERROR: {file_name} -- errors: {errors}"
            error_count += 1
        log_messages.append(msg)

    duration = round(time.time() - start_time, 2)
    report = create_processing_report(
        total_files, success_count, reject_count, error_count, duration
    )

        # Add the processing outcome metrics to the log messages
    log_messages.append("\n--- Processing Outcome Metrics ---")
    log_messages.append(f"Total Files Processed: {report['job_execution']['total_files']}")
    log_messages.append(f"Files Succeeded: {report['job_execution']['successful_files']}")
    log_messages.append(f"Files Rejected: {report['job_execution']['rejected_files']}")
    log_messages.append(f"Files Errored: {report['job_execution']['error_files']}")
    log_messages.append(f"Total Duration: {report['job_execution']['duration_seconds']} seconds")


    logger.info(json.dumps(report, indent=2))
    write_logs_to_s3(log_messages)
    publish_metrics_to_cloudwatch({
        "FilesProcessed": total_files,
        "FilesSucceeded": success_count,
        "FilesRejected": reject_count,
        "FilesErrored": error_count,
    })

if __name__ == "__main__":
    main()
