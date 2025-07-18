import json
import logging
import os
import time
import boto3
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("redshift-copy-lambda")

region = os.getenv("REGION")
redshift_workgroup = os.getenv("REDSHIFT_WORKGROUP")
redshift_db = os.getenv("REDSHIFT_DB")
redshift_table = os.getenv("REDSHIFT_TABLE")  # schema.table
s3_bucket = os.getenv("S3_BUCKET")
IAM_ROLE_ARN = os.getenv("REDSHIFT_IAM_ROLE")  # Role with Redshift S3 read access

redshift_data = boto3.client("redshift-data", region_name=region)
s3_client = boto3.client("s3", region_name=region)

def lambda_handler(event, context):
    print(f"=== LAMBDA STARTED ===")
    print(f"Event: {json.dumps(event)}")
    logger.info(f"Received event: {json.dumps(event)}")

    try:
        if 'Records' not in event:
            print("No 'Records' key in event - this might not be an S3 event")
            return {"statusCode": 200, "body": "No S3 records to process"}

        print(f"Found {len(event['Records'])} records to process")

        for i, record in enumerate(event['Records']):
            print(f"Processing record {i+1}/{len(event['Records'])}")
            print(f"Record: {json.dumps(record)}")

            s3_bucket_event = record['s3']['bucket']['name']
            s3_key = record['s3']['object']['key']
            print(f"Processing file s3://{s3_bucket_event}/{s3_key}")
            logger.info(f"Processing file s3://{s3_bucket_event}/{s3_key}")

            # Validate bucket name
            if s3_bucket_event != s3_bucket:
                print(f"ERROR: Unexpected bucket: {s3_bucket_event}, expected: {s3_bucket}")
                logger.error(f"Unexpected bucket: {s3_bucket_event}, expected: {s3_bucket}")
                continue

            s3_path = f"s3://{s3_bucket}/{s3_key}"

            # Construct COPY SQL for JSON data
            copy_sql = f"""
            COPY {redshift_table}
            FROM '{s3_path}'
            IAM_ROLE 'arn:aws:iam::985539772768:role/redshift3s3read-weblogs'
            FORMAT AS JSON 'auto'
            TRUNCATECOLUMNS
            BLANKSASNULL
            EMPTYASNULL
            COMPUPDATE OFF
            STATUPDATE OFF
            ;
            """

            logger.info(f"Executing COPY command:\n{copy_sql}")
            print(f"About to execute COPY command...")

            # Start the COPY command
            response = redshift_data.execute_statement(
                WorkgroupName=redshift_workgroup,
                Database=redshift_db,
                Sql=copy_sql
            )
            statement_id = response['Id']
            print(f"Started COPY with statement id: {statement_id}")

            # Wait for COPY completion
            print("Waiting for COPY command to finish...")
            while True:
                desc = redshift_data.describe_statement(Id=statement_id)
                status = desc['Status']
                if status in ['FINISHED', 'FAILED', 'ABORTED']:
                    break
                time.sleep(1)
            print(f"COPY command finished with status: {status}")

            if status != 'FINISHED':
                print(f"Error details: {json.dumps(desc, default=str)}")
                logger.error(f"COPY failed/aborted for s3://{s3_bucket}/{s3_key}: {desc}")
            else:
                print(f"COPY command completed successfully for {s3_path}")
                logger.info(f"COPY succeeded for s3://{s3_bucket}/{s3_key}")

    except ClientError as e:
        logger.error(f"ClientError: {e}")
        raise
    except Exception as e:
        logger.error(f"Exception: {e}")
        raise
