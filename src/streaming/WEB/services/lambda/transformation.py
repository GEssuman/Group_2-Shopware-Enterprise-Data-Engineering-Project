import json, base64, boto3, os, logging, uuid
from datetime import datetime
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

region = os.environ.get("REGION")
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
    Converts numeric timestamp (float/int) in seconds (or ms) to Redshift-compatible string.
    """
    try:
        tsf = float(ts)
        # If timestamp is in ms, convert to seconds
        if tsf > 1e11:
            tsf = tsf / 1000
        # Format as YYYY-MM-DD HH24:MI:SS
        return datetime.utcfromtimestamp(tsf).strftime("%Y-%m-%d %H:%M:%S")
    except Exception as e:
        logger.warning(f"Could not parse timestamp: {ts} ({e})")
        return None

def lambda_handler(event, context):
    valid_events = []
    anon_counter = 0  #  in-memory counter if you want anon IDs as anon001/anon002

    for record in event.get("Records", []):
        try:
            payload = base64.b64decode(record["kinesis"]["data"]).decode("utf-8")
            event_data = json.loads(payload)
            logger.info(f"Processing event: {event_data}")

            session_id = event_data.get("session_id")
            user_id = event_data.get("user_id")

            # If session_id missing/null/empty, route to failed records folder
            if not is_nonempty(session_id):
                logger.warning("Missing/empty session_id. Moving event to failed records folder.")
                fail_info = {
                    "event": event_data,
                    "error": "Missing/null/empty session_id",
                    "failed_at": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')
                }
                send_to_s3(fail_info, S3_BUCKET, "failed_transformation_records/")
                notify_failure(fail_info)
                continue

            # If user_id is missing/blank/null, generate a unique anon ID
            if not is_nonempty(user_id):
                anon_id = f"anon{uuid.uuid4().hex[:8]}"
                logger.info(f"user_id missing/null/empty - generating anon ID [{anon_id}] for analytics consistency.")
                user_id = anon_id

            # Prepare the cleaned event with formatted timestamp
            cleaned_event = {k: event_data.get(k) for k in REQUIRED_FIELDS}
            cleaned_event["user_id"] = user_id  # Overwrite with anon or real

            # Convert timestamp field to string
            if "timestamp" in cleaned_event and cleaned_event["timestamp"] is not None:
                formatted_ts = format_timestamp(cleaned_event["timestamp"])
                if formatted_ts:
                    cleaned_event["timestamp"] = formatted_ts
                else:
                    # If timestamp is unparseable, treat as failure
                    logger.warning("Malformed timestamp; moving event to failed records folder.")
                    fail_info = {
                        "event": event_data,
                        "error": f"Malformed timestamp: {cleaned_event['timestamp']}",
                        "failed_at": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')
                    }
                    send_to_s3(fail_info, S3_BUCKET, "failed_transformation_records/")
                    notify_failure(fail_info)
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

    if valid_events:
        write_ndjson_to_s3(valid_events, S3_BUCKET, "transformed_events/")

def write_ndjson_to_s3(events, bucket, prefix):
    key = prefix + f"batch_{datetime.utcnow().strftime('%Y%m%dT%H%M%S%f')}.json"
    ndjson_body = "\n".join(json.dumps(event) for event in events)
    try:
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=ndjson_body.encode("utf-8")
        )
        logger.info(f"Wrote batch of {len(events)} events to s3://{bucket}/{key}")
    except ClientError as e:
        logger.error(f"Failed to write batch to S3: {e}")
        notify_failure({
            "error": str(e),
            "batch_key": key,
            "failed_at": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')
        })

def send_to_s3(data, bucket, prefix):
    key = prefix + f"event_{datetime.utcnow().strftime('%Y%m%dT%H%M%S%f')}.json"
    try:
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(data).encode("utf-8")
        )
        logger.info(f"Saved single event to s3://{bucket}/{key}")
    except ClientError as e:
        logger.error(f"Failed to write to S3: {e}")

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
