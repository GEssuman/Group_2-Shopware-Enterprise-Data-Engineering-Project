import json
import base64
import logging
import os
import boto3
from botocore.exceptions import ClientError
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("validation-transformation-lambda")

region = os.getenv("REGION")
s3_bucket = os.getenv("S3_BUCKET")
sns_topic_arn = os.getenv("SNS_TOPIC_ARN")  # For failure alerts via SNS

s3_client = boto3.client("s3", region_name=region)
sns_client = boto3.client("sns", region_name=region)
redshift_data = boto3.client("redshift-data", region_name=region)

REDSHIFT_CLUSTER_ID = os.getenv("REDSHIFT_CLUSTER_ID")
REDSHIFT_DB = os.getenv("REDSHIFT_DB")
REDSHIFT_DB_USER = os.getenv("REDSHIFT_DB_USER")
REDSHIFT_TABLE = os.getenv("REDSHIFT_TABLE")  # Include schema.table

def notify_failure_sns(event, error_msg):
    if not sns_topic_arn:
        logger.warning("SNS_TOPIC_ARN not set; skipping SNS notification.")
        return
    
    timestamp = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')
    message = {
        "failed_at": timestamp,
        "error": error_msg,
        "event": event
    }
    try:
        sns_client.publish(
            TopicArn=sns_topic_arn,
            Subject="Validation/Transformation Failure in Lambda",
            Message=json.dumps(message, default=str),
        )
        logger.info(f"Published failure notification to SNS topic {sns_topic_arn}")
    except Exception as e:
        logger.error(f"Failed to publish SNS notification: {e}")

def save_failed(event, error_msg):
    timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S_%f')
    session_id = event.get('session_id', 'unknown')
    key = f"failed_records/{timestamp}_{session_id}.json"
    payload = {"event": event, "error": error_msg, "failed_at": timestamp}
    
    try:
        s3_client.put_object(
            Bucket=s3_bucket,
            Key=key,
            Body=json.dumps(payload).encode("utf-8"),
            ContentType="application/json"
        )
        logger.info(f"Saved failed event to s3://{s3_bucket}/{key}")
    except Exception as e:
        logger.error(f"Failed to save failed event to S3: {e}")

    # Send SNS notification for validation or transformation failure
    notify_failure_sns(event, error_msg)

def validate_event(event):
    if "session_id" not in event or not event["session_id"]:
        raise ValueError("session_id is required")
    
    if "timestamp" in event:
        try:
            ts = float(event["timestamp"])
            if ts > 1e11:  # milliseconds
                ts = ts / 1000
        except (ValueError, TypeError):
            raise ValueError("Invalid timestamp format")
    
    return event

def transform_event(event):
    if "timestamp" in event:
        try:
            ts_float = float(event["timestamp"])
            if ts_float > 1e11:
                ts_float = ts_float / 1000
            event["event_time"] = datetime.utcfromtimestamp(ts_float).strftime('%Y-%m-%d %H:%M:%S')
        except Exception:
            event["event_time"] = None
        del event["timestamp"]

    event["processed_at"] = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    return event

def save_to_redshift_batch(events):
    if not events:
        return
    
    if len(events) == 1:
        return save_to_redshift_single(events[0])
    
    columns = list(events[0].keys())
    columns_sql = ', '.join(columns)
    
    values_clauses = []
    for event in events:
        row_values = []
        for col in columns:
            val = event.get(col)
            if val is None:
                row_values.append("NULL")
            else:
                safe_val = str(val).replace("'", "''")
                row_values.append(f"'{safe_val}'")
        values_clauses.append(f"({', '.join(row_values)})")
    
    values_sql = ', '.join(values_clauses)
    sql = f"INSERT INTO {REDSHIFT_TABLE} ({columns_sql}) VALUES {values_sql};"
    
    try:
        redshift_data.execute_statement(
            ClusterIdentifier=REDSHIFT_CLUSTER_ID,
            Database=REDSHIFT_DB,
            DbUser=REDSHIFT_DB_USER,
            Sql=sql
        )
        logger.info(f"Batch inserted {len(events)} records into Redshift")
    except ClientError as e:
        logger.error(f"Redshift Data API error on batch insert: {e}")
        raise

def save_to_redshift_single(event):
    filtered_event = {k: v for k, v in event.items()}
    
    if not filtered_event:
        logger.warning("No valid data to insert")
        return
    
    columns = list(filtered_event.keys())
    values = []
    
    for col in columns:
        val = filtered_event[col]
        if val is None:
            values.append("NULL")
        else:
            safe_val = str(val).replace("'", "''")
            values.append(f"'{safe_val}'")
    
    columns_sql = ', '.join(columns)
    values_sql = ', '.join(values)
    sql = f"INSERT INTO {REDSHIFT_TABLE} ({columns_sql}) VALUES ({values_sql});"
    
    try:
        redshift_data.execute_statement(
            ClusterIdentifier=REDSHIFT_CLUSTER_ID,
            Database=REDSHIFT_DB,
            DbUser=REDSHIFT_DB_USER,
            Sql=sql
        )
        logger.info(f"Inserted record for session_id {event.get('session_id')} into Redshift")
    except ClientError as e:
        logger.error(f"Redshift Data API error on single insert: {e}")
        raise

def lambda_handler(event, context):
    try:
        records = event.get("Records", [])
        if records:
            successful_events = []
            failed_count = 0
            
            for record in records:
                if 'kinesis' in record and 'data' in record['kinesis']:
                    payload_b64 = record['kinesis']['data']
                    decoded_bytes = base64.b64decode(payload_b64)
                    event_data = json.loads(decoded_bytes)
                else:
                    event_data = record

                try:
                    validated = validate_event(event_data)
                    transformed = transform_event(validated)
                    successful_events.append(transformed)
                except Exception as e:
                    logger.error(f"Error processing record: {e}")
                    save_failed(event_data, str(e))
                    failed_count += 1
            
            if successful_events:
                try:
                    if len(successful_events) <= 5:
                        for event_data in successful_events:
                            save_to_redshift_single(event_data)
                    else:
                        save_to_redshift_batch(successful_events)
                except Exception as e:
                    logger.error(f"Redshift insert failed: {e}")
                    for event_data in successful_events:
                        save_failed(event_data, str(e))
                    failed_count += len(successful_events)

            return {
                "statusCode": 200,
                "body": json.dumps({
                    "message": f"Processed {len(records)} records.",
                    "successful": len(successful_events),
                    "failed": failed_count
                })
            }
        else:
            validated = validate_event(event)
            transformed = transform_event(validated)
            save_to_redshift_single(transformed)

            return {"statusCode": 200, "body": json.dumps({"message": "Processed single event."})}

    except Exception as e:
        logger.error(f"Fatal error: {e}")
        save_failed(event if isinstance(event, dict) else {}, str(e))
        raise
