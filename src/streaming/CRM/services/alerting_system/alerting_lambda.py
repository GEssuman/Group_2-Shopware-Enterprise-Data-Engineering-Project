import boto3
import json
import time
import logging
import os
from datetime import datetime, timedelta
from botocore.exceptions import ClientError

# Set up logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(handler)

# Configuration
REQUIRED_ENV_VARS = [
    "SNS_TOPIC_ARN",
    "ATHENA_DATABASE",
    "ATHENA_TABLE",
    "ATHENA_WORKGROUP",
    "BUCKET",
]
THRESHOLD = 10  # Number of negative interactions (rating <= 2) to trigger alert
QUERY_INTERVAL_MINUTES = 60  # Run every hour

# Validate environment variables
missing_vars = [var for var in REQUIRED_ENV_VARS if var not in os.environ]
if missing_vars:
    logger.error(f"Missing required environment variables: {', '.join(missing_vars)}")
    logger.handlers[0].flush()
    raise ValueError(
        f"Missing required environment variables: {', '.join(missing_vars)}"
    )

SNS_TOPIC_ARN = os.environ["SNS_TOPIC_ARN"]
ATHENA_DATABASE = os.environ["ATHENA_DATABASE"]
ATHENA_TABLE = os.environ["ATHENA_TABLE"]
ATHENA_WORKGROUP = os.environ["ATHENA_WORKGROUP"]
BUCKET = os.environ["BUCKET"]

# AWS Clients
athena = boto3.client("athena")
sns = boto3.client("sns")


def run_athena_query():
    """Run Athena query to count negative interactions in the last hour."""
    now = datetime.utcnow()
    start_time = (now - timedelta(hours=1)).strftime("%Y-%m-%d %H:00:00")
    query = f"""
    SELECT COUNT(*) as negative_count
    FROM {ATHENA_DATABASE}.{ATHENA_TABLE}
    WHERE rating <= 2
    AND ingestion_time >= TIMESTAMP '{start_time}'
    AND ingestion_time < TIMESTAMP '{now.strftime("%Y-%m-%d %H:00:00")}'
    """
    try:
        response = athena.start_query_execution(
            QueryString=query,
            WorkGroup=ATHENA_WORKGROUP,
            ResultConfiguration={"OutputLocation": f"s3://{BUCKET}/athena-results/"},
        )
        query_execution_id = response["QueryExecutionId"]
        logger.info(f"Started Athena query: {query_execution_id}")
        logger.handlers[0].flush()

        while True:
            status_response = athena.get_query_execution(
                QueryExecutionId=query_execution_id
            )
            status = status_response["QueryExecution"]["Status"]["State"]
            if status in ["SUCCEEDED", "FAILED", "CANCELLED"]:
                if status != "SUCCEEDED":
                    error_msg = status_response["QueryExecution"]["Status"].get(
                        "StateChangeReason", "No error details"
                    )
                    logger.error(f"Athena query failed: {error_msg}")
                    logger.handlers[0].flush()
                    return None
                break
            time.sleep(1)

        # Fetch query results
        results = athena.get_query_results(QueryExecutionId=query_execution_id)
        negative_count = int(results["ResultSet"]["Rows"][1]["Data"][0]["VarCharValue"])
        logger.info(f"Negative interactions in last hour: {negative_count}")
        logger.handlers[0].flush()
        return negative_count
    except ClientError as e:
        logger.error(f"Failed to run Athena query: {str(e)}")
        logger.handlers[0].flush()
        return None


def publish_sns_alert(negative_count):
    """Publish alert to SNS if threshold is exceeded."""
    try:
        message = f"Alert: {negative_count} negative interactions (rating <= 2) detected in the last hour, exceeding threshold of {THRESHOLD}."
        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Message=message,
            Subject="CRM Pipeline: High Negative Interaction Alert",
        )
        logger.info("Published SNS alert for negative interactions")
        logger.handlers[0].flush()
    except ClientError as e:
        logger.error(f"Failed to publish SNS alert: {str(e)}")
        logger.handlers[0].flush()


def lambda_handler(event, context):
    """Lambda handler to check negative interactions and trigger alerts."""
    negative_count = run_athena_query()
    if negative_count is None:
        logger.error("Skipping alert due to query failure")
        logger.handlers[0].flush()
        return {"statusCode": 500, "body": json.dumps({"message": "Query failed"})}
    if negative_count >= THRESHOLD:
        publish_sns_alert(negative_count)
        return {
            "statusCode": 200,
            "body": json.dumps(
                {"message": f"Alert triggered: {negative_count} negative interactions"}
            ),
        }
    logger.info("No alert triggered: negative interaction count below threshold")
    logger.handlers[0].flush()
    return {
        "statusCode": 200,
        "body": json.dumps(
            {"message": f"No alert: {negative_count} negative interactions"}
        ),
    }
