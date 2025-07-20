# Shopware Data Engineering Pipelines

This project encompasses three data pipelines for the Shopware Enterprise Data Engineering Project: the **Inventory Batch Processing Pipeline**, the **Web Analytics Pipeline**, and the **Customer Interactions Pipeline**. These pipelines process inventory, web analytics, and customer interaction data, respectively, using AWS services to ensure scalability, data quality, and compliance. Each pipeline is designed to handle specific data sources, with robust ingestion, validation, transformation, storage, and analytics capabilities.

## Overview

The pipelines collectively enable the processing, storage, and analysis of inventory, web, and customer interaction data, supporting business needs such as inventory management, user engagement tracking, and customer support analytics. They leverage AWS services like Lambda, Glue, Step Functions, Kinesis, S3, Athena, QuickSight, SNS, and CloudWatch, with Dockerized services on ECS Fargate and CI/CD via GitHub Actions.

### Pipelines
 
**Web Analytics Pipeline**:
   - Ingests web event data from an API, processes it through Kinesis, and stores it in S3 as Parquet files.
   - Computes engagement and loyalty metrics via Athena queries.
   - Runs a Dockerized API consumer on ECS Fargate.

**Customer Interactions Pipeline**:
   - Streams real-time customer interaction data from an API, processes it via Kinesis and Lambda, and stores it in S3 as Parquet files.
   - Provides real-time dashboards in QuickSight and alerts for negative interactions.
   - Deployed with CI/CD via GitHub Actions.

**Inventory Batch Processing Pipeline**:
   - Processes hourly inventory batch data (JSONL format) from an S3 source.
   - Validates, transforms, and stores data in a Delta Lake table for querying.
   - Orchestrated by AWS Step Functions, with Lambda triggers and Glue jobs.

## Architecture
![alt text](images/architecture/architecture.svg)

### Web Analytics Pipeline
- **API Consumer (`api.py`)**: Polls a web traffic API, validates events, and streams them to Kinesis.
- **Transformation Lambda (`transformation.py`)**: Processes Kinesis records, validates, transforms, and writes Parquet files to `s3://weblogs-bucket-gtp/events/` with year/month/day/hour partitioning.
- **Athena Partition Registration (`s3toathena.py`)**: Registers new S3 partitions in Athena for querying.
- **Analytics Queries (`script.sql`)**: Computes engagement (session duration, event counts) and loyalty metrics (engagement scores, loyalty rates).
- **ECS Fargate Task (`api_taskdefinition.json`, `dockerfile-api`)**: Runs the API consumer as a Dockerized service.
- **Amazon S3**: Stores events, failed records, and Athena query results.
- **Amazon SNS**: Notifies on validation failures.
- **Amazon CloudWatch**: Logs pipeline execution.

### Customer Interactions Pipeline
- **Producer (ECS Fargate, `producer.py`)**: Polls the API (`http://3.248.199.26:8000/api/customer-interaction/`) every 0.5 seconds, validates records, and streams to Kinesis.
- **Consumer Lambda (`lambda_function.py`)**: Processes Kinesis records, validates, cleans, batches (100 records), and writes Parquet files to `s3://crm-api-data-2025/processed/` with year/month/day/hour partitioning.
- **Alerting Lambda (`alerting_lambda.py`)**: Queries Athena hourly for negative interactions (`rating <= 2`) and sends SNS alerts.
- **Amazon S3**: Stores processed Parquet files, invalid records (`s3://crm-api-data-2025/invalid/`), and Athena results.
- **Amazon Athena**: Queries data in `shopware_db.crm_data`.
- **Amazon QuickSight**: Provides real-time dashboards for customer interaction trends.
- **Amazon SNS**: Sends alerts to `arn:aws:sns:us-east-1:123456789:CRMDataPipelineAlerts`.
- **Amazon CloudWatch**: Monitors Lambda errors and SQS DLQ messages.
- **GitHub Actions**: Deploys producer, consumer, and alerting components.

### Inventory Batch Processing Pipeline
- **AWS Lambda (`lambda_module.py`)**: Triggers the pipeline by detecting new JSONL files in `s3://batch-data-source-v1/inventory/`, moves them to `s3://misc-gtp-proj/landing_zone/raw/inventory/`, and starts the Step Function.
- **AWS Glue - Validation (`inventory_validate.py`)**: Validates JSONL files for schema and business rules, writing valid data as Parquet to `s3://misc-gtp-proj/landing_zone/validated/inventory/` and invalid data to `s3://misc-gtp-proj/landing_zone/rejected/inventory/`.
- **AWS Glue - Transformation (`inventory_transform.py`)**: Transforms validated data, enforces data types, deduplicates, and writes to a Delta Lake table in `s3://misc-gtp-proj/landing_zone/processed/inventory/` partitioned by date.
- **AWS Step Functions (`inventory_step_function.asl.json`)**: Orchestrates validation, transformation, and notifications.
- **Amazon S3**: Stores raw, validated, processed, rejected, and archived data, plus logs (`s3://misc-gtp-proj/logs/glue/inventory/`).
- **Amazon SNS**: Sends notifications to `arn:aws:sns:us-east-1:985539772768:inventory_alert`.
- **Amazon CloudWatch**: Logs metrics and execution details.



## Prerequisites

- **AWS Account**: Permissions for Lambda, Glue, Step Functions, Kinesis, S3, Athena, QuickSight, SNS, ECS, and CloudWatch.
- **AWS CLI**: Configured with credentials (`aws configure`).
- **Python 3.8+**: For local development and testing.
- **Docker**: For building and testing containerized services.
- **AWS SDK (boto3)**: Install via `pip install boto3`.
- **PySpark and Delta Lake**: For inventory transformation (`pip install pyspark delta-spark`).
- **Pandas and PyArrow**: For web and customer pipeline Lambda functions (`pip install pandas pyarrow`).
- **S3 Buckets**:
  - Inventory: `misc-gtp-proj` (raw, validated, processed, rejected, logs), `batch-data-source-v1` (source), `batch-archive-grp2` (archive).
  - Web Analytics: `weblogs-bucket-gtp` (events, failed records, Athena results).
  - Customer Interactions: `crm-api-data-2025` (processed, invalid, Athena results).
- **IAM Roles**:
  - Lambda/Glue roles for S3, Kinesis, Athena, SNS, Glue, Step Functions, and CloudWatch.
  - ECS task role for Kinesis, S3, and CloudWatch.
- **SNS Topics**:
  - Inventory: `arn:aws:sns:us-east-1:985539772768:inventory_alert`.
  - Web: Configurable for failure notifications.
  - Customer: `arn:aws:sns:us-east-1:123456789:CRMDataPipelineAlerts`.
- **Kinesis Streams**:
  - Web: `weblogs_stream`.
  - Customer: `CRMDataStream`.
- **SQS Queue**: `crm-dlq` for customer pipeline DLQ.

## Setup Instructions

### 1. Inventory Batch Processing Pipeline

1. **S3 Setup**:
   - Create buckets: `misc-gtp-proj`, `batch-data-source-v1`, `batch-archive-grp2`.
   - Structure: `landing_zone/raw/inventory/`, `landing_zone/validated/inventory/`, `landing_zone/processed/inventory/`, `landing_zone/rejected/inventory/`, `logs/glue/inventory/`, `state/inventory/`.

2. **Lambda Deployment**:
   - Package `lambda_module.py` with `boto3`:
     ```bash
     pip install boto3 -t lambda_package
     cp lambda_module.py lambda_package/
     cd lambda_package
     zip -r lambda_function.zip .
     ```
   - Create Lambda function (`inventory_trigger`, Python 3.8+, handler: `lambda_module.lambda_handler`).
   - Set environment variables: `STATE_MACHINE_ARN`, `PROJECT_BUCKET`, `LOG_FILE_PREFIX`, `BATCH_SOURCE_BUCKET`, `INVENTORY_SOURCE_PREFIX`.
   - Configure SQS trigger (batch size: 100, timeout: 15 min, memory: 256 MB).

3. **Glue Jobs**:
   - **Validation (`inventory_validator`)**:
     - Upload `inventory_validate.py` to `s3://misc-gtp-proj/scripts/`.
     - Create Python Shell job (Python 3.9, dependencies: `boto3`, `pandas`, `pyarrow`).
   - **Transformation (`inventory_transform`)**:
     - Upload `inventory_transform.py` to `s3://misc-gtp-proj/scripts/`.
     - Create Spark job (Glue 4.0, Delta Lake JAR: `delta-core_2.12:2.4.0`).
     - Set timeout: 120 min.

4. **Step Function**:
   - Upload `inventory_step_function.asl.json` to `s3://misc-gtp-proj/state_machines/`.
   - Create state machine (`InventoryProcessing`, Standard type).
   - Update `TopicArn` and `JobName` in ASL definition.

5. **SNS and Monitoring**:
   - Create SNS topic (`inventory_alert`).
   - Configure CloudWatch metrics: `FilesProcessed`, `FilesSucceeded`, `FilesRejected`, `FilesErrored`.

6. **Test**:
   - Upload sample JSONL to `s3://batch-data-source-v1/inventory/`:
     ```json
     {"inventory_id": 1, "product_id": 101, "warehouse_id": 201, "stock_level": 100, "restock_threshold": 20, "last_updated": 1697059200.0}
     ```
   - Trigger via SQS or S3 event, monitor logs, and query Delta Lake table.

### 2. Web Analytics Pipeline

1. **S3 and Athena Setup**:
   - Create bucket: `weblogs-bucket-gtp`.
   - Set up Athena database and table with partitions (year, month, day, hour).
   - Configure `athena_output` prefix.

2. **API Consumer (ECS Fargate)**:
   - Build Docker image:
     ```bash
     docker build -t weblogs-api -f dockerfile-api .
     ```
   - Push to ECR:
     ```bash
     aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 985539772768.dkr.ecr.us-east-1.amazonaws.com
     docker tag weblogs-api:latest 985539772768.dkr.ecr.us-east-1.amazonaws.com/weblogs/api-gtp:api
     docker push 985539772768.dkr.ecr.us-east-1.amazonaws.com/weblogs/api-gtp:api
     ```
   - Register task definition (`api_taskdefinition.json`):
     ```bash
     aws ecs register-task-definition --cli-input-json file://api_taskdefinition.json
     ```
   - Deploy ECS service.

3. **Lambda Functions**:
   - Package and deploy `transformation.py` (Kinesis trigger: `weblogs_stream`).
   - Package and deploy `s3toathena.py` (S3 trigger: `events/` prefix).
   - Set environment variables: `S3_BUCKET`, `SNS_TOPIC_ARN`, `ATHENA_DATABASE`, `ATHENA_TABLE`, `ATHENA_OUTPUT`, `REGION`.

4. **Analytics Queries**:
   - Use `script.sql` in Athena, replacing `<<$startdate>>` and `<<$enddate>>`.

5. **Monitoring**:
   - Check CloudWatch Logs for ECS and Lambda.
   - Monitor SNS for failure notifications.

### 3. Customer Interactions Pipeline

1. **S3 and Athena Setup**:
   - Create bucket: `crm-api-data-2025` (processed, invalid, athena-results).
   - Create Athena table (`shopware_db.crm_data`):
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

2. **Producer (ECS Fargate)**:
   - Build Docker image:
     ```bash
     docker build -t crm-data-producer -f Dockerfile .
     ```
   - Push to ECR:
     ```bash
     docker tag crm-data-producer:latest 985539772768.dkr.ecr.us-east-1.amazonaws.com/crm-data-producer:latest
     docker push 985539772768.dkr.ecr.us-east-1.amazonaws.com/crm-data-producer:latest
     ```
   - Register task definition (`task-definition.json`):
     ```bash
     aws ecs register-task-definition --cli-input-json file://task-definition.json
     ```
   - Deploy ECS service.

3. **Consumer Lambda**:
   - Package `lambda_function.py` with `boto3`, `pandas`, `pyarrow` (use Lambda layer: `AWSSDKPandas-Python39`).
   - Deploy function (`CRMDataConsumerLambda`, Kinesis trigger: `CRMDataStream`).
   - Set environment variables: `STREAM_NAME`, `BUCKET`, `DLQ_URL`, `ATHENA_WORKGROUP`, `ATHENA_DATABASE`, `ATHENA_TABLE`.

4. **Alerting Lambda**:
   - Package `alerting_lambda.py` with `boto3`.
   - Deploy function (`CRMDataAlertingLambda`, EventBridge trigger: hourly).
   - Set environment variables: `SNS_TOPIC_ARN`, `ATHENA_DATABASE`, `ATHENA_TABLE`, `ATHENA_WORKGROUP`, `BUCKET`.

5. **CI/CD (GitHub Actions)**:
   - Configure workflows (`deploy.yml`, `deploy-consumer.yml`, `deploy-alerting.yml`) with triggers on `feature/crm` branch.
   - Set environment variables: `AWS_REGION`, `AWS_ACCOUNT_ID`, `CRM_ECR_REPOSITORY`, `CRM_ECS_SERVICE`, `CRM_ECS_CLUSTER`, `CRM_ECS_TASK_DEFINITION`, `CRM_CONTAINER_NAME`, `LAMBDA_FUNCTION_NAME`, `S3_BUCKET`, `LAMBDA_ROLE_ARN`.

6. **QuickSight**:
   - Create dataset linked to `shopware_db.crm_data`.
   - Build dashboards for interaction trends.
   - Configure refresh settings.

7. **Monitoring**:
   - Set CloudWatch Alarms:
     - `CRMDataConsumerLambdaErrors`: `Errors` >= 1 in 5 min.
     - `CRMDataDLQMessages`: `ApproximateNumberOfMessagesVisible` >= 1 in 5 min.
   - Monitor SNS topic (`CRMDataPipelineAlerts`) for alerts.

## Usage

- **Inventory Pipeline**:
  - Input: JSONL files in `s3://batch-data-source-v1/inventory/`.
  - Output: Delta Lake table in `s3://misc-gtp-proj/landing_zone/processed/inventory/`.
  - Query via Athena for inventory KPIs (e.g., turnover, stockouts).

- **Web Analytics Pipeline**:
  - Input: Web events from API to Kinesis.
  - Output: Parquet files in `s3://weblogs-bucket-gtp/events/`.
  - Query via Athena using `script.sql` for engagement/loyalty metrics.

- **Customer Interactions Pipeline**:
  - Input: API data (`http://3.248.199.26:8000/api/customer-interaction/`).
  - Output: Parquet files in `s3://crm-api-data-2025/processed/`.
  - Visualize in QuickSight; monitor alerts for negative interactions.

## Error Handling

- **Inventory**:
  - Invalid JSONL records stored in `s3://misc-gtp-proj/landing_zone/rejected/inventory/`.
  - SNS notifications for pipeline failures.
  - CloudWatch logs for Lambda and Glue errors.

- **Web Analytics**:
  - Failed records stored in `s3://weblogs-bucket-gtp/failed_records/` and `failed_transformation_records/`.
  - SNS notifications for validation failures.
  - CloudWatch logs for ECS and Lambda.

- **Customer Interactions**:
  - Invalid records stored in `s3://crm-api-data-2025/invalid/`.
  - Failed records sent to SQS DLQ (`crm-dlq`).
  - SNS alerts for negative interactions and pipeline errors.
  - CloudWatch logs for producer, consumer, and alerting.

## Monitoring and Governance

- **CloudWatch Metrics**:
  - Inventory: `FilesProcessed`, `FilesSucceeded`, `FilesRejected`, `FilesErrored`.
  - Web: ECS and Lambda logs, SNS failure notifications.
  - Customer: Lambda errors, DLQ messages, negative interaction counts.

- **Data Quality**:
  - Inventory: Schema and business rule validation.
  - Web: Validates required fields, handles missing `user_id`.
  - Customer: Ensures non-null fields, millisecond-precision timestamps.

- **Compliance**:
  - S3 encryption (`AES256`) and TLS for data in transit.
  - IAM roles with least privilege.
  - Audit logs in S3 and CloudWatch for GDPR/CCPA compliance.

## Troubleshooting

- **Inventory**:
  - **Lambda Failures**: Check CloudWatch logs for `inventory_trigger`.
  - **Glue Failures**: Review logs for `inventory_validator`/`inventory_transform`.
  - **Data Issues**: Validate JSONL against schema in `Shopware V1.pdf`.

- **Web Analytics**:
  - **Missing Data in Athena**: Verify partitions (`SHOW PARTITIONS`), check `s3toathena.py` logs.
  - **API Failures**: Monitor `api.py` logs in CloudWatch.
  - **Invalid Events**: Check `failed_records` in S3.

- **Customer Interactions**:
  - **Athena `AlreadyExistsException`**: Ensured by `ADD IF NOT EXISTS`.
  - **Schema Mismatch (`HIVE_BAD_DATA`)**: Verify `timestamp`/`ingestion_time` as `datetime64[ms]`.
  - **QuickSight Data Missing**: Check partitions, refresh dataset.
  - **Alerts Not Triggering**: Verify CloudWatch Alarms, SNS subscriptions, `alerting_lambda.py` logs.

## Limitations

- **Inventory**: Assumes JSONL input with epoch timestamps.
- **Web Analytics**: Expects JSON API responses, epoch timestamps, and standard partitioning.
- **Customer Interactions**: Requires millisecond-precision timestamps, assumes JSON API format.

## Future Improvements

- **Inventory**:
  - Add validation rules in `inventory_validate.py`.
  - Extend Step Function for new data sources.
  - Enhance scalability for larger datasets.

- **Web Analytics**:
  - Add validation for event types.
  - Implement retry logic for Athena query failures.
  - Add throughput/latency metrics.

- **Customer Interactions**:
  - Integrate Slack/SMS alerts via SNS.
  - Add metrics for data completeness and validation failures.
  - Support schema evolution for API changes.