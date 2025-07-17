import sys
import os
from typing import List, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, to_timestamp, current_timestamp, lit
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    FloatType,
    StringType,
    TimestampType,
)
import logging
from datetime import datetime
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from delta.tables import DeltaTable
import json

# -----------------------------------------------------------------------------
# 1. Configuration and Constants
# -----------------------------------------------------------------------------

# Set up logging with more detailed formatting
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.StreamHandler(sys.stderr)
    ]
)

# AWS and S3 Configuration
PROJECT_BUCKET = os.environ.get("PROJECT_BUCKET", "misc-gtp-proj")
S3_INV_VALIDATED_PATH = f"s3://{PROJECT_BUCKET}/landing_zone/validatedinventory"
S3_INV_PROCESSED_PATH = f"s3://{PROJECT_BUCKET}/landing_zone/processed/inventory"
S3_LOG_PATH = f"s3://{PROJECT_BUCKET}/logs/glue/inventory/transform/"
MAX_RETRY_ATTEMPTS = 3

# Spark Configuration
SPARK_CONFIGS = {
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.adaptive.skewJoin.enabled": "true",
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    "spark.sql.parquet.compression.codec": "snappy",
    "spark.sql.execution.arrow.pyspark.enabled": "true",
    "spark.sql.execution.arrow.pyspark.fallback.enabled": "true"
}

# Initialize boto3 client with enhanced error handling
def create_s3_client():
    """Create S3 client with proper error handling."""
    try:
        return boto3.client(
            "s3",
            config=boto3.session.Config(
                retries={"max_attempts": MAX_RETRY_ATTEMPTS, "mode": "adaptive"}
            ),
        )
    except NoCredentialsError:
        logger.error("AWS credentials not found. Please configure your credentials.")
        raise
    except Exception as e:
        logger.error(f"Failed to create S3 client: {e}")
        raise

# -----------------------------------------------------------------------------
# 2. Helper Functions
# -----------------------------------------------------------------------------

def validate_s3_path(s3_path: str) -> bool:
    """
    Validate if S3 path exists and is accessible.
    
    Args:
        s3_path: S3 path to validate
        
    Returns:
        Boolean indicating if path is valid
    """
    try:
        s3_client = create_s3_client()
        bucket_name = s3_path.replace("s3://", "").split("/")[0]
        prefix = "/".join(s3_path.replace("s3://", "").split("/")[1:])
        
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix, MaxKeys=1)
        return response.get('KeyCount', 0) > 0
    except Exception as e:
        logger.warning(f"Could not validate S3 path {s3_path}: {e}")
        return False

def write_logs_to_s3(log_messages: List[str], job_name: str) -> None:
    """
    Writes a list of log messages to a file in S3 with enhanced error handling.

    Args:
        log_messages: A list of string messages to be written to the log.
        job_name: The name of the Glue job, used for organizing logs.
    """
    if not log_messages:
        logger.warning("No log messages to write to S3")
        return

    try:
        s3_client = create_s3_client()
        timestamp = datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")
        log_key = f"{S3_LOG_PATH.replace(f's3://{PROJECT_BUCKET}/', '')}log_{timestamp}.log"

        # Create structured log entries
        structured_logs = []
        for msg in log_messages:
            log_entry = {
                "timestamp": datetime.utcnow().isoformat(),
                "job_name": job_name,
                "message": msg,
                "level": "INFO" if not msg.startswith("ERROR") else "ERROR"
            }
            structured_logs.append(json.dumps(log_entry))

        log_content = "\n".join(structured_logs) + "\n"

        # Write the log content to S3
        s3_client.put_object(
            Bucket=PROJECT_BUCKET,
            Key=log_key,
            Body=log_content.encode("utf-8"),
            ServerSideEncryption="AES256",
            ContentType="application/json",
        )
        logger.info(f"Successfully wrote {len(log_messages)} log entries to S3: s3://{PROJECT_BUCKET}/{log_key}")
        
    except Exception as e:
        logger.error(f"Failed to write logs to S3: {e}")
        # Fallback to standard logger if S3 writing fails
        for msg in log_messages:
            if msg.startswith("ERROR"):
                logger.error(msg)
            else:
                logger.info(msg)

def create_spark_session(app_name: str) -> SparkSession:
    """
    Create Spark session with optimized configurations.
    
    Args:
        app_name: Name of the Spark application
        
    Returns:
        SparkSession instance
    """
    builder = SparkSession.builder.appName(app_name)
    
    # Apply all configurations
    for key, value in SPARK_CONFIGS.items():
        builder = builder.config(key, value)
    
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")  # Reduce log verbosity
    
    return spark

def validate_dataframe_schema(df: DataFrame, expected_columns: List[str]) -> bool:
    """
    Validate that DataFrame contains expected columns.
    
    Args:
        df: DataFrame to validate
        expected_columns: List of expected column names
        
    Returns:
        Boolean indicating if schema is valid
    """
    actual_columns = df.columns
    missing_columns = set(expected_columns) - set(actual_columns)
    
    if missing_columns:
        logger.error(f"Missing required columns: {missing_columns}")
        return False
    
    return True

def handle_delta_table_creation(df: DataFrame, output_path: str, partition_column: str) -> None:
    """
    Handle Delta table creation or append with proper error handling.
    
    Args:
        df: DataFrame to write
        output_path: S3 path for output
        partition_column: Column to partition by
    """
    try:
        # Check if Delta table exists
        if DeltaTable.isDeltaTable(df.sparkSession, output_path):
            logger.info(f"Delta table exists at {output_path}. Appending data.")
            # Use merge for better performance and deduplication
            delta_table = DeltaTable.forPath(df.sparkSession, output_path)
            
            # Simple append for now, but could be enhanced with upsert logic
            df.write.format("delta").mode("append").partitionBy(partition_column).save(output_path)
        else:
            logger.info(f"Creating new Delta table at {output_path}")
            df.write.format("delta").mode("overwrite").partitionBy(partition_column).save(output_path)
            
    except Exception as e:
        logger.error(f"Failed to write to Delta table: {e}")
        raise

# -----------------------------------------------------------------------------
# 3. Main Transformation Logic
# -----------------------------------------------------------------------------

def transform_inventory_data(df: DataFrame) -> DataFrame:
    """
    Apply business transformations to inventory data.
    
    Args:
        df: Input DataFrame
        
    Returns:
        Transformed DataFrame
    """
    try:
        # Add ingestion timestamp
        df_transformed = df.withColumn("ingestion_date", current_timestamp().cast("date"))
        df_transformed = df_transformed.withColumn("ingestion_timestamp", current_timestamp())
        
        # Add data quality indicators
        df_transformed = df_transformed.withColumn("record_processed_at", lit(datetime.utcnow().isoformat()))
        
        # Cache for performance if data will be used multiple times
        df_transformed.cache()
        
        return df_transformed
        
    except Exception as e:
        logger.error(f"Error during data transformation: {e}")
        raise

def main():
    """
    The main function for the Spark job with enhanced error handling and logging.
    """
    spark = None
    log_messages = []
    job_name = "inventory_transformation"
    start_time = datetime.utcnow()
    
    try:
        # Initialize Spark session
        spark = create_spark_session("InventoryTransformation")
        log_messages.append(f"Starting job: {job_name} at {start_time.isoformat()}")
        log_messages.append(f"Spark version: {spark.version}")
        
        # Validate input path
        if not validate_s3_path(S3_INV_VALIDATED_PATH):
            message = f"Input path does not exist or is empty: {S3_INV_VALIDATED_PATH}"
            logger.warning(message)
            log_messages.append(message)
            return
        
        # Read validated data from S3
        log_messages.append(f"Reading data from: {S3_INV_VALIDATED_PATH}")
        
        try:
            df = spark.read.format("parquet").load(S3_INV_VALIDATED_PATH)
            
            # Validate DataFrame is not empty
            if df.rdd.isEmpty():
                message = "No data found in source path"
                logger.warning(message)
                log_messages.append(message)
                return
            
        except Exception as e:
            error_msg = f"Failed to read data from {S3_INV_VALIDATED_PATH}: {str(e)}"
            logger.error(error_msg)
            log_messages.append(f"ERROR: {error_msg}")
            raise
        
        # Get record count with caching for performance
        df.cache()
        record_count = df.count()
        log_messages.append(f"Successfully read {record_count} records.")
        
        if record_count == 0:
            log_messages.append("No new data to process. Exiting job.")
            return
        
        # Log schema information
        log_messages.append(f"DataFrame schema: {df.schema.simpleString()}")
        
        # Transform the data
        log_messages.append("Starting data transformation...")
        df_transformed = transform_inventory_data(df)
        
        # Validate transformed data
        transformed_count = df_transformed.count()
        log_messages.append(f"Transformation complete. Record count: {transformed_count}")
        
        if transformed_count != record_count:
            logger.warning(f"Record count mismatch: input={record_count}, transformed={transformed_count}")
        
        # Write to Delta Lake
        log_messages.append(f"Writing transformed data to Delta Lake at: {S3_INV_PROCESSED_PATH}")
        
        # Optimize write operations
        df_transformed.coalesce(1).write.format("delta").mode("append").partitionBy("ingestion_date").save(S3_INV_PROCESSED_PATH)
        
        log_messages.append("Data successfully written to Delta Lake.")
        
        # Cleanup cached DataFrames
        df.unpersist()
        df_transformed.unpersist()
        
    except Exception as e:
        error_message = f"An error occurred during job execution: {str(e)}"
        logger.error(error_message, exc_info=True)
        log_messages.append(f"ERROR: {error_message}")
        
        # Re-raise the exception to ensure job fails
        raise
        
    finally:
        # Final log entry and cleanup
        end_time = datetime.utcnow()
        duration = end_time - start_time
        log_messages.append(f"Job finished at {end_time.isoformat()}. Total duration: {duration}")
        
        # Write logs to S3
        write_logs_to_s3(log_messages, job_name)
        
        # Stop Spark session
        if spark:
            spark.stop()
            logger.info("Spark session stopped successfully")

if __name__ == "__main__":
    try:
        main()
        logger.info("Job completed successfully")
        ## sys.exit(0)
    except Exception as e:
        logger.error(f"Job failed with error: {e}", exc_info=True)
        sys.exit(1)