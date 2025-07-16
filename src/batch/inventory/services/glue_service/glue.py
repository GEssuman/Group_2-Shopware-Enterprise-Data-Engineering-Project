import sys
import boto3
import json
import logging
from datetime import datetime
from botocore.exceptions import ClientError
from typing import Set, List, Tuple, Optional, Dict
import os
import time
import pandas as pd
import io
from awsglue.utils import getResolvedOptions

# -----------------------------------------------------------------------------
# 1. Configuration and Constants
# -----------------------------------------------------------------------------

# Set up comprehensive logging
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# --- S3 Configuration ---
S3_BUCKET = "misc-gtp-proj"
S3_RAW_PATH = f"s3://{S3_BUCKET}/inventory_landing_zone/raw"
S3_REJECTED_PATH = f"s3://{S3_BUCKET}/inventory_landing_zone/rejected"
S3_STATE_PATH = f"s3://{S3_BUCKET}/inventory_landing_zone/state/processed_files.json"
S3_LOG_PATH = f"s3://{S3_BUCKET}/logs/glue/"

# --- File Processing Configuration ---
FILE_SIZE_THRESHOLD = 104857600  # 100MB
CHUNK_SIZE = 52428800  # 50MB
MAX_PARALLEL_FILES = 5
MAX_RETRY_ATTEMPTS = 3

# --- Schema Definition ---
REQUIRED_COLUMNS = {"inventory_id", "product_id", "warehouse_id", "stock_level", "last_updated"}

# Expected data types for validation
EXPECTED_DTYPES = {
    "inventory_id": "int64",
    "product_id": "int64", 
    "warehouse_id": "int64",
    "stock_level": "int64",
    "restock_threshold": "Int64",  # Nullable integer
    "last_updated": "float64"
}

# Initialize boto3 clients with retry configuration
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
# 2. Enhanced Helper Functions
# -----------------------------------------------------------------------------

def retry_operation(operation, max_attempts: int = MAX_RETRY_ATTEMPTS, delay: float = 1.0):
    """
    Retry decorator for operations that might fail transiently.
    """
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
        return last_exception
    return wrapper

def load_processed_files_state() -> Set[str]:
    """
    Loads the set of previously processed file keys from the state file in S3.
    """
    @retry_operation
    def _load_state():
        try:
            state_key = S3_STATE_PATH.replace(f"s3://{S3_BUCKET}/", "")
            response = s3_client.get_object(Bucket=S3_BUCKET, Key=state_key)
            state_data = json.loads(response['Body'].read().decode('utf-8'))
            processed_files = set(state_data.get("processed_files", []))
            logger.info(f"Loaded state with {len(processed_files)} processed files")
            return processed_files
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                logger.info("State file not found. Starting with empty state.")
                return set()
            else:
                logger.error(f"Error loading state file: {e}")
                raise
    
    return _load_state()

def save_processed_files_state(processed_keys: Set[str]) -> None:
    """
    Saves the updated set of processed file keys to the state file in S3.
    """
    @retry_operation
    def _save_state():
        try:
            state_data = {
                "last_updated": datetime.utcnow().isoformat(),
                "processed_files": sorted(list(processed_keys)),
                "total_files": len(processed_keys)
            }
            
            state_key = S3_STATE_PATH.replace(f"s3://{S3_BUCKET}/", "")
            s3_client.put_object(
                Bucket=S3_BUCKET,
                Key=state_key,
                Body=json.dumps(state_data, indent=2),
                ServerSideEncryption='AES256',
                ContentType='application/json'
            )
            
            logger.info(f"Successfully saved state for {len(processed_keys)} files")
        except ClientError as e:
            logger.error(f"Failed to save state file: {e}")
            raise
    
    _save_state()

def write_logs_to_s3(log_messages: List[str]) -> None:
    """
    Writes collected log messages to a date-stamped log file in S3.
    """
    if not log_messages:
        return
    
    @retry_operation
    def _write_logs():
        try:
            timestamp = datetime.utcnow().strftime('%Y-%m-%d_%H')
            log_key = f"{S3_LOG_PATH.replace(f's3://{S3_BUCKET}/', '')}validation_log_{timestamp}.log"
            
            log_content = "\n".join(log_messages) + "\n"
            
            existing_content = ""
            try:
                response = s3_client.get_object(Bucket=S3_BUCKET, Key=log_key)
                existing_content = response['Body'].read().decode('utf-8')
            except ClientError as e:
                if e.response['Error']['Code'] != 'NoSuchKey':
                    raise
            
            s3_client.put_object(
                Bucket=S3_BUCKET,
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

def get_new_files_to_process(incoming_path: str, processed_files: Set[str]) -> List[Dict]:
    """
    Lists objects in the incoming S3 path and returns file information.
    """
    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        incoming_bucket, incoming_prefix = incoming_path.replace("s3://", "").split('/', 1)
        
        new_files = []
        for page in paginator.paginate(Bucket=incoming_bucket, Prefix=incoming_prefix):
            for obj in page.get('Contents', []):
                key = obj['Key']
                if not key.endswith('/') and key not in processed_files:
                    file_info = {
                        'key': key,
                        'size': obj['Size'],
                        'last_modified': obj['LastModified'],
                        'needs_chunking': obj['Size'] > FILE_SIZE_THRESHOLD
                    }
                    new_files.append(file_info)
        
        logger.info(f"Found {len(new_files)} new files to process")
        large_files = [f for f in new_files if f['needs_chunking']]
        if large_files:
            logger.info(f"Found {len(large_files)} large files requiring chunking")
        
        return new_files
    
    except Exception as e:
        logger.error(f"Error listing files from {incoming_path}: {e}")
        raise

# -----------------------------------------------------------------------------
# 3. Pandas-based Data Processing Functions
# -----------------------------------------------------------------------------

def read_jsonl_from_s3(file_key: str) -> pd.DataFrame:
    """
    Read a JSONL file from S3 and return as pandas DataFrame.
    """
    try:
        response = s3_client.get_object(Bucket=S3_BUCKET, Key=file_key)
        content = response['Body'].read().decode('utf-8')
        
        # Parse JSON lines
        records = []
        for line_num, line in enumerate(content.strip().split('\n'), 1):
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
    """
    Read a large JSONL file from S3 in chunks and return as pandas DataFrame.
    """
    logger.info(f"Reading large file {file_key} in chunks")
    
    all_records = []
    chunk_num = 0
    
    for start_byte in range(0, file_size, CHUNK_SIZE):
        end_byte = min(start_byte + CHUNK_SIZE - 1, file_size - 1)
        
        try:
            byte_range = f"bytes={start_byte}-{end_byte}"
            response = s3_client.get_object(
                Bucket=S3_BUCKET,
                Key=file_key,
                Range=byte_range
            )
            
            chunk_content = response['Body'].read().decode('utf-8')
            
            # Handle partial JSON lines at chunk boundaries
            if chunk_num > 0:
                lines = chunk_content.split('\n')[1:]  # Skip incomplete first line
            else:
                lines = chunk_content.split('\n')
                
            if start_byte + CHUNK_SIZE < file_size:
                lines = lines[:-1]  # Remove incomplete last line
            
            # Process lines in this chunk
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
    """
    Validate pandas DataFrame against expected schema and business rules.
    """
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
            logger.warning(f"Found extra columns (will be ignored): {', '.join(sorted(extra_cols))}")
        
        # Type conversion and validation
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
        
        # Business rule validation
        business_valid, business_errors = validate_business_rules_pandas(validated_df)
        if not business_valid:
            errors.extend(business_errors)
            return False, errors
        
        logger.info(f"Validation passed for {len(validated_df)} rows")
        return True, []
        
    except Exception as e:
        errors.append(f"Validation failed with exception: {str(e)}")
        logger.error(f"Validation error: {e}")
        return False, errors

def validate_business_rules_pandas(df: pd.DataFrame) -> Tuple[bool, List[str]]:
    """
    Validate business-specific rules using pandas.
    """
    errors = []
    
    try:
        # Rule 1: stock_level should be >= 0
        negative_stock = (df['stock_level'] < 0).sum()
        if negative_stock > 0:
            errors.append(f"Found {negative_stock} rows with negative stock levels")
        
        # Rule 2: restock_threshold should be >= 0 when not null
        if 'restock_threshold' in df.columns:
            invalid_threshold = ((df['restock_threshold'].notna()) & 
                               (df['restock_threshold'] < 0)).sum()
            if invalid_threshold > 0:
                errors.append(f"Found {invalid_threshold} rows with negative restock thresholds")
        
        # Rule 3: last_updated should be reasonable (not too far in future)
        current_timestamp = datetime.utcnow().timestamp()
        future_threshold = current_timestamp + 86400  # 24 hours in future
        future_updates = (df['last_updated'] > future_threshold).sum()
        if future_updates > 0:
            errors.append(f"Found {future_updates} rows with future last_updated timestamps")
        
        return len(errors) == 0, errors
        
    except Exception as e:
        errors.append(f"Business rule validation failed: {str(e)}")
        return False, errors

def write_dataframe_to_s3_parquet(df: pd.DataFrame, output_path: str) -> None:
    """
    Write pandas DataFrame to S3 as Parquet format.
    """
    try:
        # Convert DataFrame to parquet bytes
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, engine='pyarrow', index=False)
        parquet_buffer.seek(0)
        
        # Generate filename
        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        s3_key = f"{output_path.replace(f's3://{S3_BUCKET}/', '')}/processed_{timestamp}.parquet"
        
        # Upload to S3
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=parquet_buffer.getvalue(),
            ServerSideEncryption='AES256',
            ContentType='application/octet-stream'
        )
        
        logger.info(f"Successfully wrote {len(df)} rows to s3://{S3_BUCKET}/{s3_key}")
        
    except Exception as e:
        logger.error(f"Failed to write DataFrame to S3: {e}")
        raise

# -----------------------------------------------------------------------------
# 4. Enhanced File Processing
# -----------------------------------------------------------------------------

def move_s3_object(source_key: str, dest_path: str) -> None:
    """
    Move an S3 object with enhanced error handling and retry logic.
    """
    @retry_operation
    def _move_object():
        try:
            file_name = source_key.split('/')[-1]
            dest_bucket, dest_prefix = dest_path.replace("s3://", "").split('/', 1)
            
            # Add timestamp to avoid conflicts
            timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
            name_parts = file_name.rsplit('.', 1)
            if len(name_parts) == 2:
                dest_key = f"{dest_prefix.rstrip('/')}/{name_parts[0]}_{timestamp}.{name_parts[1]}"
            else:
                dest_key = f"{dest_prefix.rstrip('/')}/{file_name}_{timestamp}"
            
            # Copy object
            copy_source = {'Bucket': S3_BUCKET, 'Key': source_key}
            s3_client.copy_object(
                CopySource=copy_source,
                Bucket=dest_bucket,
                Key=dest_key,
                ServerSideEncryption='AES256'
            )
            
            # Delete original
            s3_client.delete_object(Bucket=S3_BUCKET, Key=source_key)
            
            logger.info(f"Successfully moved {source_key} to {dest_key}")
            
        except ClientError as e:
            logger.error(f"Failed to move {source_key}: {e}")
            raise
    
    _move_object()

def process_single_file(file_info: Dict) -> Tuple[bool, List[str], str]:
    """
    Process a single file using pandas instead of Spark.
    """
    file_key = file_info['key']
    logger.info(f"Processing file: {file_key} (Size: {file_info['size']} bytes)")
    
    try:
        # Read file based on size
        if file_info['needs_chunking']:
            df = read_chunked_jsonl_from_s3(file_key, file_info['size'])
        else:
            df = read_jsonl_from_s3(file_key)
        
        if df.empty:
            return False, ["File is empty"], "EMPTY"
        
        # Validate data
        is_valid, errors = validate_dataframe(df)
        
        if is_valid:
            # Write to raw zone as parquet
            write_dataframe_to_s3_parquet(df, S3_RAW_PATH)
            
            # Delete original file
            s3_client.delete_object(Bucket=S3_BUCKET, Key=file_key)
            
            return True, [], "SUCCESS"
        else:
            # Move to rejected zone
            move_s3_object(file_key, S3_REJECTED_PATH)
            return False, errors, "REJECTED"
            
    except Exception as e:
        logger.error(f"Critical error processing {file_key}: {e}")
        try:
            move_s3_object(file_key, S3_REJECTED_PATH)
        except:
            logger.error(f"Could not move failing file {file_key} to rejected zone")
        return False, [f"Processing failed: {str(e)}"], "ERROR"

# -----------------------------------------------------------------------------
# 5. Monitoring and Reporting Functions
# -----------------------------------------------------------------------------

def publish_metrics_to_cloudwatch(metrics: Dict):
    """
    Publish custom metrics to CloudWatch for monitoring.
    """
    try:
        cloudwatch = boto3.client('cloudwatch')
        
        metric_data = []
        for metric_name, value in metrics.items():
            metric_data.append({
                'MetricName': metric_name,
                'Value': value,
                'Unit': 'Count',
                'Dimensions': [
                    {
                        'Name': 'JobName',
                        'Value': 'inventory-validation-job'
                    }
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
    """
    Create a comprehensive processing report.
    """
    report = {
        'job_execution': {
            'start_time': datetime.utcnow().isoformat(),
            'duration_seconds': duration,
            'status': 'COMPLETED'
        },
        'file_processing': {
            'total_files': total_files,
            'successful_files': successful_files,
            'rejected_files': rejected_files,
            'error_files': error_files,
            'success_rate': (successful_files / total_files * 100) if total_files > 0 else 0
        },
        'configuration': {
            'file_size_threshold': FILE_SIZE_THRESHOLD,
            'chunk_size': CHUNK_SIZE,
            'max_retry_attempts': MAX_RETRY_ATTEMPTS
        }
    }
    
    return report

def save_processing_report(report: Dict):
    """
    Save the processing report to S3.
    """
    try:
        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        report_key = f"reports/processing_report_{timestamp}.json"
        
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=report_key,
            Body=json.dumps(report, indent=2),
            ServerSideEncryption='AES256',
            ContentType='application/json'
        )
        
        logger.info(f"Saved processing report to s3://{S3_BUCKET}/{report_key}")
        
    except Exception as e:
        logger.error(f"Failed to save processing report: {e}")

def create_dead_letter_queue_entry(file_key: str, errors: List[str]):
    """
    Create an entry in a dead letter queue for files that repeatedly fail.
    """
    try:
        dlq_entry = {
            'file_key': file_key,
            'timestamp': datetime.utcnow().isoformat(),
            'errors': errors,
            'retry_count': 0,
            'status': 'FAILED'
        }
        
        dlq_key = f"dead_letter_queue/{datetime.utcnow().strftime('%Y/%m/%d')}/{file_key.replace('/', '_')}.json"
        
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=dlq_key,
            Body=json.dumps(dlq_entry, indent=2),
            ServerSideEncryption='AES256',
            ContentType='application/json'
        )
        
        logger.info(f"Created dead letter queue entry for {file_key}")
        
    except Exception as e:
        logger.error(f"Failed to create DLQ entry for {file_key}: {e}")

def validate_configuration():
    """
    Validate job configuration and environment variables.
    """
    errors = []
    
    # Validate S3 bucket access
    try:
        s3_client.head_bucket(Bucket=S3_BUCKET)
    except ClientError as e:
        errors.append(f"Cannot access S3 bucket {S3_BUCKET}: {e}")
    
    # Validate configuration values
    if FILE_SIZE_THRESHOLD < 1024:
        errors.append(f"FILE_SIZE_THRESHOLD too small: {FILE_SIZE_THRESHOLD}")
    
    if CHUNK_SIZE < 1024:
        errors.append(f"CHUNK_SIZE too small: {CHUNK_SIZE}")
    
    if CHUNK_SIZE >= FILE_SIZE_THRESHOLD:
        errors.append("CHUNK_SIZE should be smaller than FILE_SIZE_THRESHOLD")
    
    if MAX_RETRY_ATTEMPTS < 1 or MAX_RETRY_ATTEMPTS > 10:
        errors.append(f"MAX_RETRY_ATTEMPTS should be between 1 and 10: {MAX_RETRY_ATTEMPTS}")
    
    if errors:
        logger.error("Configuration validation failed:")
        for error in errors:
            logger.error(f" - {error}")
        raise ValueError("Invalid configuration")
    
    logger.info("Configuration validation passed")

# -----------------------------------------------------------------------------
# 6. Main Function
# -----------------------------------------------------------------------------

def main():
    """
    Main function for Glue Python Shell job.
    """
    # Get job arguments
    args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_INCOMING_PATH'])
    
    s3_incoming_path = args['S3_INCOMING_PATH']
    s3_logs = []
    start_time = datetime.utcnow()
    
    logger.info(f"Starting Python Shell validation job for path: {s3_incoming_path}")
    s3_logs.append(f"[{start_time.isoformat()}] [INFO] Starting validation job")
    
    try:
        # Step 1: Load state and discover files
        processed_files_state = load_processed_files_state()
        files_to_process = get_new_files_to_process(s3_incoming_path, processed_files_state)
        
        if not files_to_process:
            logger.info("No new files to process")
            s3_logs.append(f"[{datetime.utcnow().isoformat()}] [INFO] No new files found")
            write_logs_to_s3(s3_logs)
            return
        
        # Step 2: Process files with statistics tracking
        total_files = len(files_to_process)
        successful_files = 0
        rejected_files = 0
        error_files = 0
        
        logger.info(f"Processing {total_files} files")
        s3_logs.append(f"[{datetime.utcnow().isoformat()}] [INFO] Processing {total_files} files")
        
        # Process files sequentially
        for i, file_info in enumerate(files_to_process, 1):
            file_key = file_info['key']
            logger.info(f"Processing file {i}/{total_files}: {file_key}")
            
            try:
                success, errors, status = process_single_file(file_info)
                
                # Update statistics
                if status == "SUCCESS":
                    successful_files += 1
                    s3_logs.append(f"[{datetime.utcnow().isoformat()}] [INFO] SUCCESS: {file_key}")
                elif status == "REJECTED":
                    rejected_files += 1
                    s3_logs.append(f"[{datetime.utcnow().isoformat()}] [WARN] REJECTED: {file_key} - {'; '.join(errors)}")
                    create_dead_letter_queue_entry(file_key, errors)
                else:
                    error_files += 1
                    s3_logs.append(f"[{datetime.utcnow().isoformat()}] [ERROR] ERROR: {file_key} - {'; '.join(errors)}")
                    create_dead_letter_queue_entry(file_key, errors)
                
                # Add to processed state
                processed_files_state.add(file_key)
                
                # Periodically save state
                if i % 10 == 0:
                    save_processed_files_state(processed_files_state)
                    logger.info(f"Saved intermediate state after processing {i} files")
                
            except Exception as e:
                error_files += 1
                logger.error(f"Unexpected error processing {file_key}: {e}")
                s3_logs.append(f"[{datetime.utcnow().isoformat()}] [ERROR] UNEXPECTED: {file_key} - {str(e)}")
                create_dead_letter_queue_entry(file_key, [str(e)])
                processed_files_state.add(file_key)
        
        # Step 3: Final statistics and cleanup
        end_time = datetime.utcnow()
        duration = (end_time - start_time).total_seconds()
        
        logger.info(f"Job completed in {duration:.2f} seconds")
        logger.info(f"Results: {successful_files} successful, {rejected_files} rejected, {error_files} errors")
        
        s3_logs.append(f"[{end_time.isoformat()}] [INFO] Job completed in {duration:.2f}s")
        s3_logs.append(f"[{end_time.isoformat()}] [INFO] Results: {successful_files} successful, {rejected_files} rejected, {error_files} errors")
        
        # Create and save processing report
        report = create_processing_report(total_files, successful_files, rejected_files, error_files, duration)
        save_processing_report(report)
        
        # Publish metrics to CloudWatch
        metrics = {
            'TotalFiles': total_files,
            'SuccessfulFiles': successful_files,
            'RejectedFiles': rejected_files,
            'ErrorFiles': error_files,
            'ProcessingDuration': duration
        }
        publish_metrics_to_cloudwatch(metrics)
        
        # Final state save
        save_processed_files_state(processed_files_state)
        write_logs_to_s3(s3_logs)
        
    except Exception as e:
        logger.error(f"Critical job failure: {e}")
        s3_logs.append(f"[{datetime.utcnow().isoformat()}] [CRITICAL] Job failed: {str(e)}")
        write_logs_to_s3(s3_logs)
        raise

if __name__ == "__main__":
    # Install required packages for Python Shell
    import subprocess
    import sys
    
    # Install pandas and pyarrow if not available
    try:
        import pandas as pd
        import pyarrow
    except ImportError:
        subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'pandas', 'pyarrow'])
        import pandas as pd
        import pyarrow
    
    # Validate configuration before starting
    validate_configuration()
    
    # Log configuration for debugging
    logger.info("Job Configuration:")
    logger.info(f" FILE_SIZE_THRESHOLD: {FILE_SIZE_THRESHOLD} bytes")
    logger.info(f" CHUNK_SIZE: {CHUNK_SIZE} bytes")
    logger.info(f" MAX_RETRY_ATTEMPTS: {MAX_RETRY_ATTEMPTS}")
    
    main()
