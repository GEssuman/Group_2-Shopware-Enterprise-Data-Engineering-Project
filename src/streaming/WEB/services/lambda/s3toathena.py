import json
import logging
import os
import time
import boto3
import re
import urllib.parse
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

region = os.getenv("REGION", "us-east-1")
athena_database = os.getenv("ATHENA_DATABASE")    # Glue database name
athena_table = os.getenv("ATHENA_TABLE")          # Glue table name
s3_bucket = os.getenv("S3_BUCKET")                 # Bucket with partitioned data
athena_output = os.getenv("ATHENA_OUTPUT")         # S3 location for Athena query results e.g. s3://your-query-results-bucket/prefix/

athena_client = boto3.client("athena", region_name=region)

def lambda_handler(event, context):
    logger.info(f"Received event: {json.dumps(event)}")
    if 'Records' not in event:
        logger.warning("No records found in event")
        return {'statusCode': 200, 'body': 'No records to process'}

    for record in event['Records']:
        # Extract bucket & key from event record
        s3_bucket_event = record['s3']['bucket']['name']
        s3_key_encoded = record['s3']['object']['key']

        # Decode URL-encoded S3 key (e.g. %3D => =)
        s3_key = urllib.parse.unquote_plus(s3_key_encoded)

        logger.info(f"Processing S3 object: s3://{s3_bucket_event}/{s3_key}")

        # Only process if bucket matches expected bucket
        if s3_bucket_event != s3_bucket:
            logger.warning(f"Event bucket {s3_bucket_event} does not match expected bucket {s3_bucket}. Skipping.")
            continue

        # Extract partition info from decoded key using regex:
        # expecting path like: events/year=2025/month=07/day=18/hour=13/...
        partition_pattern = (
            r"events/"
            r"year=(?P<year>\d{4})/"
            r"month=(?P<month>\d{1,2})/"
            r"day=(?P<day>\d{1,2})/"
            r"hour=(?P<hour>\d{1,2})/"
        )

        match = re.search(partition_pattern, s3_key)
        if not match:
            logger.warning(f"Could not extract partitions from key: {s3_key}. Skipping.")
            continue

        year = match.group("year")
        month = match.group("month").zfill(2)
        day = match.group("day").zfill(2)
        hour = match.group("hour").zfill(2)

        # Location of the partition (prefix up to hour=XX/)
        partition_location = f"s3://{s3_bucket}/events/year={year}/month={month}/day={day}/hour={hour}/"
        logger.info(f"Registering partition for {year}-{month}-{day} hour {hour} at location {partition_location}")

        # Construct Athena DDL to add partition (with IF NOT EXISTS)
        ddl = f"""
        ALTER TABLE {athena_table}
        ADD IF NOT EXISTS PARTITION (
            year='{year}',
            month='{month}',
            day='{day}',
            hour='{hour}'
        )
        LOCATION '{partition_location}'
        """

        try:
            # Execute the Athena query
            query_id = start_athena_query(ddl, athena_database, athena_output)
            logger.info(f"Started Athena query {query_id} for partition registration.")

            # Optionally wait for query completion and log status
            wait_for_query_to_complete(query_id)

        except Exception as e:
            logger.error(f"Error running Athena partition registration query: {e}")

    return {'statusCode': 200, 'body': 'Partition registration(s) complete'}

    logger.info(f"Using Athena output location: {athena_output}")

def start_athena_query(query, database, output_location):
    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': database},
        ResultConfiguration={'OutputLocation': output_location}
    )
    return response['QueryExecutionId']

def wait_for_query_to_complete(query_execution_id, wait_interval_sec=2):
    while True:
        response = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        status = response['QueryExecution']['Status']['State']
        if status == 'SUCCEEDED':
            logger.info(f"Athena query {query_execution_id} succeeded")
            return
        elif status in ['FAILED', 'CANCELLED']:
            failure_reason = response['QueryExecution']['Status'].get('StateChangeReason', 'Unknown')
            logger.error(f"Athena query {query_execution_id} failed/cancelled: {failure_reason}")
            raise Exception(f"Query failed: {failure_reason}")
        else:
            logger.info(f"Waiting for Athena query {query_execution_id} to complete. Current status: {status}")
            time.sleep(wait_interval_sec)
