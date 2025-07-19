# Customer Intractions

## Overview

This document describes the real-time streaming data pipeline designed to extract customer interaction data from the API endpoint, process it, store it in a structured format, and make it available for analysis in AWS QuickSight. The pipeline enables the Customer Support Team to visualize real-time customer interaction metrics via dashboards and receive alerts for critical events, supporting timely decision-making.

The pipeline is built using AWS services (ECS, Kinesis, Lambda, S3, Athena, QuickSight, SNS, CloudWatch) and leverages GitHub Actions for CI/CD. It includes robust error handling, validation, retry mechanisms, and alerting to ensure data reliability and availability.

## Architecture

The pipeline consists of the following components:

1. **Producer (ECS Fargate Task)**:

   - Polls the API endpoint (`http://3.248.199.26:8000/api/customer-interaction/`) every 0.5 seconds.
   - Validates records and sends valid ones to a Kinesis Data Stream.
   - Stores invalid records in S3 for debugging.
   - Deployed as a Docker container via ECS Fargate.

2. **Consumer (Lambda Function)**:

   - Reads records from the Kinesis Data Stream.
   - Validates, cleans, and batches records (up to 100 per batch).
   - Writes batches to S3 as Parquet files with partitioning.
   - Updates Athena partitions for querying.
   - Sends failed records to an SQS Dead Letter Queue (DLQ).

3. **Alerting (Lambda Function and CloudWatch Alarms)**:

   - Monitors negative interactions (`rating <= 2`) via an Athena query, triggered hourly.
   - Monitors Lambda errors and DLQ messages using CloudWatch Alarms.
   - Sends notifications via SNS to the Customer Support Team.

4. **Storage (S3)**:

   - Stores processed data as Parquet files in `s3://crm-api-data-2025/processed/`.
   - Stores invalid records as JSON in `s3://crm-api-data-2025/invalid/`.
   - Stores Athena query results in `s3://crm-api-data-2025/athena-results/`.

5. **Querying (Athena)**:

   - Queries data in the `shopware_db.crm_data` table, partitioned by `year`, `month`, `day`, and `hour`.

6. **Visualization (QuickSight)**:

   - Connects to the Athena table (`shopware_db.crm_data`) for real-time dashboards.

7. **CI/CD (GitHub Actions)**:
   - Deploys the producer to ECS via Docker image updates to ECR.
   - Deploys the consumer and alerting Lambdas.

## Components

### 1. Producer (ECS Fargate Task)

**Purpose**: Fetches customer interaction data from the API and sends it to Kinesis.

**Source File**: `producer.py`

**Key Features**:

- **Polling**: Queries the API every 0.5 seconds (`POLL_INTERVAL_SECONDS`).
- **Validation**: Ensures records have required fields (`customer_id`, `interaction_type`, `timestamp`) and non-null values.
- **Kinesis Integration**: Sends valid records to the Kinesis Data Stream (`CRMDataStream`) with `customer_id` as the partition key.
- **Error Handling**: Saves invalid records to S3 (`s3://crm-api-data-2025/invalid/`) as JSON with error details.
- **Retries**: Uses exponential backoff (up to 5 retries) for API and Kinesis operations.

**Dependencies** (`requirements.txt`):

- `boto3`, `requests`, `python-dotenv`

**Configuration** (`task-definition.json`):

- **Task Family**: `CRMDataProducerTask`
- **Container Name**: `crm-data-producer`
- **Image**: `985539772768.dkr.ecr.us-east-1.amazonaws.com/crm-data-producer:latest`
- **Environment Variables**:
  - `STREAM_NAME`, `BUCKET`, `API_URL`
- **Logging**: CloudWatch Logs (`/ecs/CRMDataProducerTask`)

**Dockerfile**:

- Base image: `python:3.9-slim`
- Installs dependencies from `requirements.txt`
- Copies `producer.py` and runs it as the entrypoint.

### 2. Consumer (Lambda Function)

**Purpose**: Processes Kinesis records, writes them to S3 as Parquet, and updates Athena partitions.

**Source File**: `lambda_function.py`

**Key Features**:

- **Record Processing**:
  - Decodes base64-encoded Kinesis records and parses JSON.
  - Validates records against the schema
  - Cleans records by trimming strings, normalizing nulls, and validating ranges.
- **Parquet Writing**:
  - Converts batches to `pandas` DataFrame, truncating `timestamp` and `ingestion_time` to millisecond precision (`datetime64[ms]`).
  - Uses `pyarrow` schema with `pa.timestamp('ms')` for `timestamp` and `ingestion_time`.
  - Writes Parquet files to `s3://crm-api-data-2025/processed/year=YYYY/month=MM/day=DD/hour=HH/{batch_id}.parquet`.
- **Athena Integration**: Updates partitions using `ALTER TABLE ADD IF NOT EXISTS PARTITION`.
- **Error Handling**: Sends failed records to the SQS DLQ (`crm-dlq`) with error details.
- **Retries**: Uses exponential backoff (up to 5 retries) for S3 and Athena operations.

**Dependencies**:

- `boto3==1.28.57`, `pandas` and `pyarrow` (provided via a custom Lambda layer, e.g., `AWSSDKPandas-Python39`)

**Configuration**:

- **Environment Variables**:
  - `STREAM_NAME`, `BUCKET`, `DLQ_URL`, `ATHENA_WORKGROUP`,`ATHENA_DATABASE`, `ATHENA_TABLE`
- **IAM Role**: Permissions for S3 (`PutObject`), SQS (`SendMessage`), Athena (`StartQueryExecution`, `GetQueryExecution`), and Glue (`GetDatabase`, `UpdatePartition`).

### 3. Alerting System

**Purpose**: Monitors pipeline health and data quality, sending notifications for critical events.

**Components**:

- **CloudWatch Alarms**:
  - **Lambda Errors**: Monitors `Errors` metric for `CRMDataConsumerLambda`.
  - **DLQ Messages**: Monitors `ApproximateNumberOfMessagesVisible` for `crm-dlq`.
  - **Producer API Failures** (optional): Monitors custom `APIFailures` metric in `CRMDataPipeline` namespace.
- **Alerting Lambda**: Queries Athena for negative interactions (`rating <= 2`) hourly and triggers SNS notifications.
- **SNS Topic**: Sends email notifications to the Customer Support Team.

**Source File**: `alerting_lambda.py`

**Key Features**:

- **Negative Interaction Monitoring**:
  - Runs an Athena query hourly to count interactions with `rating <= 2` in the last hour.
  - Triggers an SNS notification if the count exceeds 10 (`THRESHOLD`).
- **Scheduling**: Triggered by an EventBridge rule every hour.
- **Error Handling**: Logs query failures and skips alerts if queries fail.
- **Notification**: Publishes to SNS topic `CRMDataPipelineAlerts` with details (e.g., "Alert: 11 negative interactions (rating <= 2) detected in the last hour").

**Dependencies**:

- `boto3==1.28.57`

**Configuration**:

- **Lambda Function**: `CRMDataAlertingLambda`
- **Environment Variables**:
  - `SNS_TOPIC_ARN`, `ATHENA_DATABASE`, `ATHENA_TABLE`, `ATHENA_WORKGROUP`, `BUCKET`: `crm-api-data-2025`
- **IAM Permissions**: SNS (`Publish`), Athena (`StartQueryExecution`, `GetQueryExecution`, `GetQueryResults`), S3 (`PutObject`, `GetObject`).

**CloudWatch Alarms**:

- **CRMDataConsumerLambdaErrors**:
  - Metric: `Errors` (AWS/Lambda)
  - Threshold: >= 1 error in 5 minutes
  - Action: Publish to `CRMDataPipelineAlerts`
- **CRMDataDLQMessages**:
  - Metric: `ApproximateNumberOfMessagesVisible` (AWS/SQS)
  - Threshold: >= 1 message in 5 minutes
  - Action: Publish to `CRMDataPipelineAlerts`

**SNS Topic**:

- ARN: `arn:aws:sns:us-east-1:123456789:CRMDataPipelineAlerts`
- Subscribed Emails: `support-team@example.com`

### 4. Storage (S3)

**Bucket**: `crm-api-data-2025`

**Structure**:

- **Processed Data**: `s3://crm-api-data-2025/processed/year=YYYY/month=MM/day=DD/hour=HH/{batch_id}.parquet`
- **Invalid Records**: `s3://crm-api-data-2025/invalid/year=YYYY/month=MM/day=DD/{uuid}.json`
  - JSON format with `error`, `record`, and `timestamp` fields.
- **Athena Results**: `s3://crm-api-data-2025/athena-results/`
  - Stores query output from Athena (used by consumer and alerting Lambda).

**Compression**: Parquet files use SNAPPY compression (`TBLPROPERTIES ('parquet.compression'='SNAPPY')`).

### 5. Querying (Athena)

**Database**: `shopware_db`

**Table**: `crm_data`

**Schema**:

```sql
CREATE EXTERNAL TABLE shopware_db.crm_data (
    customer_id INT,
    interaction_type STRING,
    timestamp TIMESTAMP,
    channel STRING,
    rating INT,
    message_excerpt STRING,
    ingestion_time TIMESTAMP
)
PARTITIONED BY (year INT, month INT, day INT, hour INT)
STORED AS PARQUET
LOCATION 's3://crm-api-data-2025/processed/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');
```

**Partitioning**:

- Partitions are created by the consumer Lambda using `ALTER TABLE ADD IF NOT EXISTS PARTITION`.
- Example partition: `year=2025/month=7/day=18/hour=16`.

**Example Query**:

```sql
SELECT * FROM shopware_db.crm_data
WHERE year = 2025 AND month = 7 AND day = 18 AND hour = 16
LIMIT 10;
```

### 6. Visualization (QuickSight)

**Purpose**: Provides real-time dashboards for the Customer Support Team.

<img src="Images/CRM/Customer Support Dashboard.png" alt="Customer Support Dashboard">

**Configuration**:

- Connects to `shopware_db.crm_data` via Athena.

**Setup**:

1. Create a QuickSight dataset linked to `shopware_db.crm_data`.
2. Configure refresh settings.
3. Build visualizations (e.g., time-series charts, bar graphs) for customer interaction trends.

### 7. CI/CD (GitHub Actions)

**Producer Deployment** (`deploy.yml`):

- **Trigger**: Push to `feature/crm` branch for `src/streaming/CRM/services/producer/**` or `deploy.yml`.
- **Steps**:
  1. Checks out the repository.
  2. Configures AWS credentials.
  3. Logs in to Amazon ECR.
  4. Builds and pushes the Docker image to ECR (`985539772768.dkr.ecr.us-east-1.amazonaws.com/crm-data-producer:{git_sha}`).
  5. Updates the ECS task definition with the new image.
  6. Deploys to the ECS cluster and service, waiting for stability.
- **Environment Variables**:
  - `AWS_REGION`, `AWS_ACCOUNT_ID`, `CRM_ECR_REPOSITORY`, `CRM_ECS_SERVICE`, `CRM_ECS_CLUSTER`, `CRM_ECS_TASK_DEFINITION`, `CRM_CONTAINER_NAME`.

**Consumer Deployment** (`deploy-consumer.yml`):

- **Trigger**: Push to `feature/crm` branch for `src/streaming/CRM/services/consumer/**` or `deploy-consumer.yml`.
- **Steps**:
  1. Checks out the repository.
  2. Configures AWS credentials.
  3. Sets up Python 3.9.
  4. Installs `boto3==1.28.57` into a package directory.
  5. Copies `lambda_function.py` and zips it.
  6. Uploads the zip to S3 (`crm-api-data-2025/lambda_function.zip`).
  7. Updates the Lambda function (`CRMDataConsumerLambda`) with the new code.
  8. Waits for the Lambda update to complete.
- **Environment Variables**:
  - `AWS_REGION`, `AWS_ACCOUNT_ID`, `LAMBDA_FUNCTION_NAME`, `S3_BUCKET`, `LAMBDA_ROLE_ARN`.

**Alerting Deployment** (`deploy-alerting.yml`):

- **Trigger**: Push to `feature/crm` branch for `src/streaming/CRM/services/alerting/**` or `deploy-alerting.yml`.
- **Steps**:
  1. Checks out the repository.
  2. Configures AWS credentials.
  3. Sets up Python 3.9.
  4. Installs `boto3==1.28.57` into a package directory.
  5. Copies `alerting_lambda.py` and zips it.
  6. Uploads the zip to S3 (`crm-api-data-2025/alerting_lambda.zip`).
  7. Updates the Lambda function (`CRMDataAlertingLambda`) with the new code.
  8. Waits for the Lambda update to complete.
- **Environment Variables**:
  - `AWS_REGION`, `AWS_ACCOUNT_ID`, `LAMBDA_FUNCTION_NAME`, `S3_BUCKET`, `LAMBDA_ROLE_ARN`.

## Operational Details

### Monitoring

- **CloudWatch Logs**:

  - Producer: `/ecs/CRMDataProducerTask`
  - Consumer: `/aws/lambda/CRMDataConsumerLambda`
  - Alerting: `/aws/lambda/CRMDataAlertingLambda`
  - Look for:
    - `Successfully sent X records to Kinesis` (producer).
    - `Stored micro-batch {batch_id} to S3` (consumer).
    - `Added Athena partition for batch {batch_id}` (consumer).
    - `Published SNS alert for negative interactions` (alerting).

- **CloudWatch Alarms**:

  - `CRMDataConsumerLambdaErrors`: Monitors Lambda errors.
  - `CRMDataDLQMessages`: Monitors DLQ message counts.
  - Check alarm states in the CloudWatch Console.

- **QuickSight**: Verify data appears in dashboards after partition updates.

## Troubleshooting

### Common Issues and Fixes

1. **Athena `AlreadyExistsException`**:

   - **Cause**: Attempting to add an existing partition.
   - **Fix**: Ensured by using `ADD IF NOT EXISTS` in the consumer Lambda.

2. **Athena `HIVE_BAD_DATA` (Schema Mismatch)**:

   - **Cause**: `timestamp` and `ingestion_time` stored as `BINARY` (strings) instead of `TIMESTAMP`.
   - **Fix**: Modified consumer Lambda to store as `datetime` objects with `timestamp[ms]` precision.

3. **Lambda `Casting from timestamp[ns] to timestamp[ms]` Error**:

   - **Cause**: Nanosecond precision in `datetime64[ns]` conflicting with `pa.timestamp('ms')`.
   - **Fix**: Added `.dt.floor('ms')` to truncate timestamps to millisecond precision.

4. **Data Not in QuickSight**:

   - **Cause**: Missing partitions, failed Lambda executions, or dataset refresh issues.
   - **Fix**:
     - Verify partitions in Athena (`SHOW PARTITIONS shopware_db.crm_data`).
     - Check CloudWatch logs for errors.
     - Refresh QuickSight dataset.

5. **Alerts Not Triggering**:
   - **Cause**: Incorrect alarm thresholds, query failures, or SNS subscription issues.
   - **Fix**:
     - Verify alarm thresholds and metrics in CloudWatch.
     - Check `CRMDataAlertingLambda` logs for query or SNS errors.
     - Confirm SNS subscriptions are active (`aws sns list-subscriptions-by-topic`).

## IAM Permissions

### Producer Role (`CRMProducerTaskRole`)

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["kinesis:PutRecords"],
      "Resource": "arn:aws:kinesis:us-east-1:985539772768:stream/CRMDataStream"
    },
    {
      "Effect": "Allow",
      "Action": ["s3:PutObject"],
      "Resource": "arn:aws:s3:::crm-api-data-2025/invalid/*"
    },
    {
      "Effect": "Allow",
      "Action": ["logs:CreateLogStream", "logs:PutLogEvents"],
      "Resource": "arn:aws:logs:us-east-1:985539772768:log-group:/ecs/CRMDataProducerTask:*"
    }
  ]
}
```

### Consumer and Alerting Role (`CRMConsumerLambdaRole`)

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:PutObject"],
      "Resource": "arn:aws:s3:::crm-api-data-2025/processed/*"
    },
    {
      "Effect": "Allow",
      "Action": ["sqs:SendMessage"],
      "Resource": "arn:aws:sqs:us-east-1:985539772768:crm-dlq"
    },
    {
      "Effect": "Allow",
      "Action": [
        "athena:StartQueryExecution",
        "athena:GetQueryExecution",
        "athena:GetQueryResults"
      ],
      "Resource": "arn:aws:athena:us-east-1:985539772768:workgroup/*"
    },
    {
      "Effect": "Allow",
      "Action": ["glue:GetDatabase", "glue:UpdatePartition"],
      "Resource": [
        "arn:aws:glue:us-east-1:985539772768:catalog",
        "arn:aws:glue:us-east-1:985539772768:database/shopware_db",
        "arn:aws:glue:us-east-1:985539772768:table/shopware_db/crm_data"
      ]
    },
    {
      "Effect": "Allow",
      "Action": ["s3:PutObject", "s3:GetObject"],
      "Resource": "arn:aws:s3:::crm-api-data-2025/athena-results/*"
    },
    {
      "Effect": "Allow",
      "Action": ["kinesis:GetRecords", "kinesis:GetShardIterator"],
      "Resource": "arn:aws:kinesis:us-east-1:985539772768:stream/CRMDataStream"
    },
    {
      "Effect": "Allow",
      "Action": ["sns:Publish"],
      "Resource": "arn:aws:sns:us-east-1:985539772768:CRMDataPipelineAlerts"
    }
  ]
}
```

## Future Enhancements

- **Advanced Alerting**: Integrate with Slack or SMS via SNS for broader notification reach.
- **Custom Metrics**: Add more CloudWatch metrics (e.g., invalid record counts, processing latency).
- **Data Quality**: Add metrics for data completeness and validation failure rates in QuickSight.
