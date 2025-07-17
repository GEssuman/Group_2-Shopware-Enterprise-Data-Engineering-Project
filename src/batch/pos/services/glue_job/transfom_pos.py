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
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'POS_S3_BUCKET', 'POS_DELTA_BUCKET'])
delta_path = f"s3://{args['POS_DELTA_BUCKET']}/pos"

s3_input_path = f"s3://{args['POS_S3_BUCKET']}/pos"

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


def main():
    df = read_csv_files(s3_input_path)
    df = df.withColumn("timestamp", F.from_unixtime(F.col("timestamp")).cast("timestamp"))\
            .withColumn("date", F.to_date("timestamp"))

    df.write.format("delta") \
            .partitionBy("date") \
            .mode("append") \
            .save(delta_path)

if __name__ == "__main__":
    main()