import json
import base64
import boto3
import os
import logging
import uuid
import io
from datetime import datetime
from collections import defaultdict
from botocore.exceptions import ClientError

import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger()
logger.setLevel(logging.INFO)

region = os.environ.get("REGION", "us-east-1")
S3_BUCKET = os.environ.get("S3_BUCKET")
SNS_TOPIC_ARN = os.environ.get("SNS_TOPIC_ARN")

s3 = boto3.client("s3", region_name=region)
sns = boto3.client("sns", region_name=region)

REQUIRED_FIELDS = [
    "session_id", "user_id", "page",
    "device_type", "browser", "event_type", "timestamp"
]

def is_nonempty(val):
    if val is None:
        return False
    if isinstance(val, str) and val.strip() == "":
        return False
    return True

def format_timestamp(ts):
    """
    Convert numeric epoch timestamp (seconds or milliseconds) to datetime.datetime object (UTC).
    """
    try:
        tsf = float(ts)
        # Convert milliseconds to seconds if needed
        if tsf > 1e11:
            tsf = tsf / 1000
        return datetime.utcfromtimestamp(tsf)
    except Exception as e:
        logger.warning(f"Could not parse timestamp: {ts} ({e})")
        return None

def lambda_handler(event, context):
    valid_events = []
    failed_count = 0

    for record in event.get("Records", []):
        try:
            payload = base64.b64decode(record["kinesis"]["data"]).decode("utf-8")
            event_data = json.loads(payload)
            logger.info(f"Processing event: {event_data}")

            session_id = event_data.get("session_id")
            user_id = event_data.get("user_id")

            if not is_nonempty(session_id):
                fail_info = {
                    "event": event_data,
                    "error": "Missing/null/empty session_id",
                    "failed_at": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')
                }
                logger.warning(fail_info["error"])
                send_to_s3(fail_info, S3_BUCKET, "failed_transformation_records/")
                notify_failure(fail_info)
                failed_count += 1
                continue

            if not is_nonempty(user_id):
                anon_id = f"anon{uuid.uuid4().hex[:8]}"
                logger.info(f"user_id missing/null/empty - generating anon ID [{anon_id}]")
                user_id = anon_id

            cleaned_event = {k: event_data.get(k) for k in REQUIRED_FIELDS}
            cleaned_event["user_id"] = user_id

            # Convert timestamp -> event_time (datetime object), remove original timestamp
            if "timestamp" in cleaned_event and cleaned_event["timestamp"] is not None:
                dt_obj = format_timestamp(cleaned_event["timestamp"])
                if dt_obj:
                    cleaned_event["event_time"] = dt_obj
                    del cleaned_event["timestamp"]
                else:
                    fail_info = {
                        "event": event_data,
                        "error": f"Malformed timestamp: {cleaned_event['timestamp']}",
                        "failed_at": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')
                    }
                    logger.warning(fail_info["error"])
                    send_to_s3(fail_info, S3_BUCKET, "failed_transformation_records/")
                    notify_failure(fail_info)
                    failed_count += 1
                    continue
            else:
                fail_info = {
                    "event": event_data,
                    "error": "Missing timestamp",
                    "failed_at": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')
                }
                logger.warning(fail_info["error"])
                send_to_s3(fail_info, S3_BUCKET, "failed_transformation_records/")
                notify_failure(fail_info)
                failed_count += 1
                continue

            valid_events.append(cleaned_event)

        except Exception as e:
            fail_info = {
                "error": str(e),
                "raw": record,
                "failed_at": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')
            }
            send_to_s3(fail_info, S3_BUCKET, "failed_records/")
            logger.error(f"Error processing record: {e}")
            notify_failure(fail_info)
            failed_count += 1

    if valid_events:
        write_parquet_partitioned(valid_events, S3_BUCKET)

    logger.info(f"Lambda processing complete. Valid events: {len(valid_events)}, Failed events: {failed_count}")

def write_parquet_partitioned(events, bucket):
    partitions = defaultdict(list)
    for ev in events:
        dt = ev.get("event_time")
        if isinstance(dt, str):
            dt = datetime.fromisoformat(dt)
        ev["event_time"] = dt  # ensure datetime for Arrow
        partitions[(dt.year, dt.month, dt.day, dt.hour)].append(ev)

    schema = pa.schema([
        ("session_id", pa.string()),
        ("user_id", pa.string()),
        ("page", pa.string()),
        ("device_type", pa.string()),
        ("browser", pa.string()),
        ("event_type", pa.string()),
        ("event_time", pa.timestamp('ms'))
    ])

    for (year, month, day, hour), partition_events in partitions.items():
        # Normalize string fields to string type before writing parquet
        for ev in partition_events:
            for key in ['session_id', 'user_id', 'page', 'device_type', 'browser', 'event_type']:
                val = ev.get(key)
                ev[key] = str(val) if val is not None else None

        table = pa.Table.from_pylist(partition_events, schema=schema)

        buffer = io.BytesIO()
        pq.write_table(table, buffer, compression='snappy')
        buffer.seek(0)

        s3_key = (
            f"events/year={year:04d}/month={month:02d}/day={day:02d}/hour={hour:02d}/"
            f"batch_{datetime.utcnow().strftime('%Y%m%dT%H%M%S%f')}.parquet"
        )
        s3.put_object(Bucket=bucket, Key=s3_key, Body=buffer.read())
        logger.info(f"Wrote {len(partition_events)} events as Parquet to s3://{bucket}/{s3_key}")

def send_to_s3(data, bucket, prefix):
    key = prefix + f"event_{datetime.utcnow().strftime('%Y%m%dT%H%M%S%f')}.json"
    try:
        s3.put_object(Bucket=bucket, Key=key, Body=json.dumps(data).encode("utf-8"))
        logger.info(f"Saved failure event to s3://{bucket}/{key}")
    except ClientError as e:
        logger.error(f"Failed to write failure event to S3: {e}")

def notify_failure(message):
    if not SNS_TOPIC_ARN:
        logger.warning("SNS_TOPIC_ARN not set; skipping SNS notification.")
        return
    try:
        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject="Lambda Processing Failure",
            Message=json.dumps(message, indent=2)
        )
    except ClientError as e:
        logger.error(f"Failed to send SNS notification: {e}")
