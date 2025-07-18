import sys
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DoubleType
from datetime import datetime
from delta.tables import DeltaTable
import json
import boto3
import logging

#  Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]: %(message)s")

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session


# Get job arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'POS_S3_BUCKET', 'ETL_LANDING_S3_BUCKET','ARCHIVE_BUCKET'])
delta_path = f"s3://{args['ETL_LANDING_S3_BUCKET']}/landing_zone/processed/pos"
archive_bucket = args['ARCHIVE_BUCKET']
source_bucket = args['POS_S3_BUCKET']
s3_input_path = f"s3://{source_bucket}/pos"

schema = StructType([
    StructField("transaction_id", StringType()),
    StructField("store_id", IntegerType()),
    StructField("product_id", IntegerType()),
    StructField("quantity", IntegerType()),
    StructField("revenue", FloatType()),
    StructField("discount_applied", FloatType()),
    StructField("timestamp", FloatType())
])

def read_csv_files(path):
    """Read all CSV files from the specified S3 path."""
    try:
        logging.info(f"Reading CSV files from: {path}")
        df = spark.read \
            .schema(schema) \
            .option("header", "true") \
            .csv(path)

        logging.info(f"Loaded {df.count()} records from CSV files.")
        return df

    except Exception as e:
        logging.error(f"Failed to read CSVs from {path}: {e}")
        raise

def archive_all_csv_files(source_bucket, prefix, archive_bucket):
    """
    Archive all CSV files under a given prefix in an S3 bucket.

    :param bucket_name: str, e.g., "ecommerce-raw.amalitech-gke"
    :param prefix: str, e.g., "orders/" or "products/"
    :param archive_prefix: str, archive base folder (defaults to 'archive/')
    """
    logging.info(f"Archiving files in bucket: {source_bucket}, prefix: {prefix}")
    s3 = boto3.client("s3")

    try:
        response = s3.list_objects_v2(Bucket=source_bucket, Prefix=prefix)
        if "Contents" not in response:
            logging.info(f"No files found under {prefix}")
            return

        for obj in response["Contents"]:
            key = obj["Key"]
            if key.endswith(".csv"):
                dest_key = prefix + key
                logging.info(f"Archiving {key} → {dest_key}")
                
                # Copy to archive
                s3.copy_object(
                    Bucket=archive_bucket,
                    CopySource={"Bucket": source_bucket, "Key": key},
                    Key=dest_key
                )

                # Delete original
                s3.delete_object(Bucket=source_bucket, Key=key)

        logging.info(f"All CSV files under '{prefix}' archived successfully.")

    except Exception as e:
        logging.error(f"Failed to archive files under {prefix}: {e}")



def main():
    df = read_csv_files(s3_input_path)
    df = df.withColumn("timestamp", F.from_unixtime(F.col("timestamp")).cast("timestamp"))\
            .withColumn("date", F.to_date("timestamp"))
    df = df.dropDuplicates()
    df.write.format("delta") \
            .partitionBy("date") \
            .mode("append") \
            .save(delta_path)
    
    archive_all_csv_files(source_bucket, "pos", archive_bucket)

if __name__ == "__main__":
    main()