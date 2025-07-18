import sys
import os
from typing import List, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, to_timestamp, current_timestamp, lit, from_unixtime, to_date
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
S3_INV_VALIDATED_PATH = f"s3://{PROJECT_BUCKET}/landing_zone/validated/inventory"
S3_INV_PROCESSED_PATH = f"s3://{PROJECT_BUCKET}/landing_zone/processed/inventory"
S3_LOG_PATH = f"s3://{PROJECT_BUCKET}/logs/glue/inventory/transform/"
RAW_SOURCE_PATH = "s3://misc-gtp-proj/landing_zone/raw/inventory/"
ARCHIVE_DEST_PATH = "s3://batch-archive-grp2/inventory/"
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




def upsert_inventory_delta(df: DataFrame, output_path: str, partition_column: str, merge_key: str = "inventory_id") -> None:
    """
    Always upsert data using Delta Lake. Repartition only if large batch; otherwise upsert as-is.
    """
    spark = df.spark
    batch_size = df.count()  # This triggers computation (expensive but necessary here)

    logger.info(f"Batch size: {batch_size}")

    # Only repartition if the batch is large
    if batch_size > 10_000_000 and df.rdd.getNumPartitions() < 200:
        logger.info("Large batch detected. Repartitioning to 200 partitions for parallel upsert.")
        df = df.repartition(200)
    # For small batches, use as-is (no explicit coalesce or repartition)

    if DeltaTable.isDeltaTable(spark, output_path):
        delta_table = DeltaTable.forPath(spark, output_path)
        merge_condition = f"target.{merge_key} = source.{merge_key}"

        delta_table.alias("target").merge(
            df.alias("source"),
            merge_condition
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()
        logger.info("Upsert (merge) into existing Delta table complete.")
    else:
        df.write.format("delta").partitionBy(partition_column).mode("overwrite").save(output_path)
        logger.info("Delta table did not exist. Created new with initial data.")


def delete_s3_files(s3_path: str, log_messages: Optional[list] = None) -> None:
    """
    Deletes all objects under the specified S3 path.
    Args:
        s3_path: The S3 path from which to delete files (e.g., s3://bucket/prefix/)
        log_messages: List to append log info/warnings to.
    """
    s3_client = create_s3_client()
    try:
        bucket_name, prefix = s3_path.replace("s3://", "").split("/", 1)
        paginator = s3_client.get_paginator('list_objects_v2')
        deleted_count = 0
        
        # Collect all object keys to delete
        objects_to_delete = []
        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
            if "Contents" in page:
                for obj in page["Contents"]:
                    objects_to_delete.append({"Key": obj["Key"]})

        if not objects_to_delete:
            msg = f"No files found to delete in {s3_path}"
            logger.info(msg)
            if log_messages is not None:
                log_messages.append(msg)
            return

        # Delete in batches of 1000 (S3 API limit)
        for i in range(0, len(objects_to_delete), 1000):
            batch = objects_to_delete[i:i + 1000]
            s3_client.delete_objects(
                Bucket=bucket_name,
                Delete={"Objects": batch, "Quiet": True} # Quiet=True means no error for non-existent objects
            )
            deleted_count += len(batch)
            
        msg = f"Successfully deleted {deleted_count} files from {s3_path}"
        logger.info(msg)
        if log_messages is not None:
            log_messages.append(msg)

    except Exception as e:
        error_msg = f"Failed to delete files from {s3_path}: {e}"
        logger.error(error_msg)
        if log_messages is not None:
            log_messages.append(f"ERROR: {error_msg}")




def archive_s3_inventory_files(src_path: str, dest_path: str, log_messages: Optional[list] = None) -> None:
    """
    Moves all S3 objects from src_path to dest_path.
    src_path: full S3 prefix (e.g., s3://misc-gtp-proj/landing_zone/raw/inventory/)
    dest_path: full S3 prefix (e.g., s3://batch-archive-grp2/inventory/)
    """
    s3_client = create_s3_client()
    try:
        src_bucket, src_prefix = src_path.replace("s3://", "").split("/", 1)
        dest_bucket, dest_prefix = dest_path.replace("s3://", "").split("/", 1)
        paginator = s3_client.get_paginator('list_objects_v2')
        moved_files = 0

        for page in paginator.paginate(Bucket=src_bucket, Prefix=src_prefix):
            for obj in page.get("Contents", []):
                src_key = obj["Key"]
                if not src_key.endswith('/'):  # skip folders
                    rel_key = src_key[len(src_prefix):].lstrip("/")
                    dest_key = f"{dest_prefix.rstrip('/')}/{rel_key}"
                    # Copy
                    s3_client.copy_object(
                        Bucket=dest_bucket,
                        CopySource={'Bucket': src_bucket, 'Key': src_key},
                        Key=dest_key,
                        ServerSideEncryption="AES256"
                    )
                    # Delete original
                    s3_client.delete_object(Bucket=src_bucket, Key=src_key)
                    moved_files += 1
        msg = f"Archived {moved_files} raw files from {src_path} to {dest_path}"
        logger.info(msg)
        if log_messages is not None:
            log_messages.append(msg)
    except Exception as e:
        error_msg = f"Failed to archive raw inventory files from {src_path} to {dest_path}: {e}"
        logger.error(error_msg)
        if log_messages is not None:
            log_messages.append(f"ERROR: {error_msg}")



# -----------------------------------------------------------------------------
# 3. Main Transformation Logic
# -----------------------------------------------------------------------------


def transform_inventory_data(df: DataFrame) -> DataFrame:
    """
    Transform inventory data with type conversions and derived fields.
    """
    try:
        df_typed = (
            df
            .withColumn("inventory_id", col("inventory_id").cast(IntegerType()))
            .withColumn("product_id", col("product_id").cast(IntegerType()))
            .withColumn("warehouse_id", col("warehouse_id").cast(IntegerType()))
            .withColumn("stock_level", col("stock_level").cast(IntegerType()))
            .withColumn("restock_threshold", col("restock_threshold").cast(IntegerType()))
            .withColumn("last_updated", col("last_updated").cast(FloatType()))
            .withColumn(
                "last_updated_datetime",
                from_unixtime(col("last_updated")).cast(TimestampType())
            )
            .withColumn(
                "last_updated_date",
                to_date(from_unixtime(col("last_updated")))
            )
        )
        df_typed.cache()
        return df_typed
    except Exception as e:
        logger.error(f"Error during data transformation: {e}")
        raise


def main():
    """
    Main function for the inventory transformation Spark job:
    - Validates input
    - Reads batch data
    - Transforms and enforces data types
    - Dynamically optimizes write to Delta Lake partitioned by last_updated_date
    """
    spark = None
    log_messages = []
    job_name = "inventory_transformation"
    start_time = datetime.utcnow()

    try:
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

        df.cache()
        record_count = df.count()
        log_messages.append(f"Successfully read {record_count} records.")

        if record_count == 0:
            log_messages.append("No new data to process. Exiting job.")
            return

        log_messages.append(f"DataFrame schema: {df.schema.simpleString()}")

        # ---- START Transform ----
        log_messages.append("Starting data transformation...")
        # transform_inventory_data must enforce type conversion and add last_updated_date!
        df_transformed = transform_inventory_data(df)
        # ---- END Transform ----

        transformed_count = df_transformed.count()
        log_messages.append(f"Transformation complete. Record count: {transformed_count}")

        if transformed_count != record_count:
            logger.warning(f"Record count mismatch: input={record_count}, transformed={transformed_count}")

        # ---- Writing ----
        log_messages.append(f"Writing transformed data to Delta Lake at: {S3_INV_PROCESSED_PATH}")
        upsert_inventory_delta(df_transformed, S3_INV_PROCESSED_PATH, "last_updated_date", "inventory_id")
        
        
        # ARCHIVE RAW FILES after upsert  
        try:
            log_messages.append(f"Archiving raw inventory files from {RAW_SOURCE_PATH} to {ARCHIVE_DEST_PATH}")
            archive_s3_inventory_files(RAW_SOURCE_PATH, ARCHIVE_DEST_PATH, log_messages)
        except Exception as e:
            archive_error = f"ERROR archiving raw files: {e}"
            logger.error(archive_error)
            log_messages.append(archive_error)
        

        # DELETE VALIDATED FILES after successful processing and raw archiving
        try:
            log_messages.append(f"Deleting validated files from {S3_INV_VALIDATED_PATH}")
            delete_s3_files(S3_INV_VALIDATED_PATH, log_messages)
        except Exception as e:
            delete_error = f"ERROR deleting validated files: {e}"
            logger.error(delete_error)
            log_messages.append(delete_error)


        df.unpersist()
        df_transformed.unpersist()

    except Exception as e:
        error_message = f"An error occurred during job execution: {str(e)}"
        logger.error(error_message, exc_info=True)
        log_messages.append(f"ERROR: {error_message}")
        raise

    finally:
        end_time = datetime.utcnow()
        duration = end_time - start_time
        log_messages.append(f"Job finished at {end_time.isoformat()}. Total duration: {duration}")
        write_logs_to_s3(log_messages, job_name)
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