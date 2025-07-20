# Shopware Inventory Batch Processing Pipeline

This project implements a robust, scalable data pipeline for processing hourly inventory batch data in the Shopware Enterprise Data Engineering Project. The pipeline handles ingestion, validation, transformation, and storage of inventory data using AWS services, ensuring data quality, reliability, and compliance.

## Overview

The inventory batch pipeline processes hourly inventory data files, performing validation, transformation, and storage in a Delta Lake table. The pipeline is orchestrated using AWS Step Functions, with AWS Lambda for triggering, AWS Glue for processing, and Amazon S3 for storage. The system is designed to meet the requirements outlined in the Shopware project, focusing on scalability, data quality, and compliance.

### Components
- **AWS Lambda (`lambda_module.py`)**: Triggers the pipeline by detecting new files in the source S3 bucket, moves them to the raw landing zone, and initiates the Step Function.
- **AWS Glue - Validation (`inventory_validate.py`)**: Validates incoming JSONL files for schema compliance and business rules, writing valid data to a validated zone and invalid data to a rejected zone.
- **AWS Glue - Transformation (`inventory_transform.py`)**: Transforms validated data, enforces data types, deduplicates records, and writes to a Delta Lake table partitioned by date.
- **AWS Step Functions (`inventory_step_function.asl.json`)**: Orchestrates the pipeline, coordinating validation, transformation, and notifications.
- **Amazon S3**: Stores raw, validated, processed, rejected, and archived data, along with logs.
- **Amazon SNS**: Sends success/failure notifications.
- **Amazon CloudWatch**: Logs metrics and pipeline execution details.

## Prerequisites

- **AWS Account**: Access to an AWS account with permissions to create and manage Lambda, Glue, S3, Step Functions, SNS, and CloudWatch resources.
- **AWS CLI**: Configured with credentials for programmatic access.
- **Python 3.8+**: Required for local development and testing.
- **AWS SDK (boto3)**: For interacting with AWS services.
- **PySpark and Delta Lake**: Required for the transformation job (`inventory_transform.py`).
- **S3 Bucket**: A bucket (e.g., `misc-gtp-proj`) for storing data and logs.
- **IAM Roles**:
  - Lambda execution role with permissions for S3, Step Functions, and CloudWatch.
  - Glue execution role with permissions for S3, Glue, and CloudWatch.
- **SNS Topic**: An SNS topic 

System: It appears the response was cut off. I'll complete the README.md for the inventory batch pipeline, focusing on setup details and adhering to the artifact guidelines. Below is the complete artifact with the requested content.

<xaiArtifact artifact_id="b9d4ffc7-1406-4ddc-a2ea-1915cd7929cc" artifact_version_id="b4c29a1a-b279-4c4b-b58a-e69c015858d1" title="README.md" contentType="text/markdown">

# Shopware Inventory Batch Processing Pipeline

This project implements a robust, scalable data pipeline for processing hourly inventory batch data in the Shopware Enterprise Data Engineering Project. The pipeline handles ingestion, validation, transformation, and storage of inventory data using AWS services, ensuring data quality, reliability, and compliance.

## Overview

The inventory batch pipeline processes hourly inventory data files in JSONL format, performing validation, transformation, and storage in a Delta Lake table. The pipeline is orchestrated using AWS Step Functions, with AWS Lambda for triggering, AWS Glue for processing, and Amazon S3 for storage. It meets the Shopware project requirements for scalability, data quality, and compliance, specifically for the inventory data source (batch, hourly).

### Components
- **AWS Lambda (`lambda_module.py`)**: Triggers the pipeline by detecting new files in the source S3 bucket (`batch-data-source-v1/inventory/`), moves them to the raw landing zone (`s3://misc-gtp-proj/landing_zone/raw/inventory/`), and initiates the Step Function.
- **AWS Glue - Validation (`inventory_validate.py`)**: Validates incoming JSONL files for schema compliance and business rules, writing valid data as Parquet to the validated zone (`s3://misc-gtp-proj/landing_zone/validated/inventory/`) and invalid data to the rejected zone (`s3://misc-gtp-proj/landing_zone/rejected/inventory/`).
- **AWS Glue - Transformation (`inventory_transform.py`)**: Transforms validated Parquet data, enforces data types, deduplicates records, adds derived fields, and writes to a Delta Lake table (`s3://misc-gtp-proj/landing_zone/processed/inventory/`) partitioned by date.
- **AWS Step Functions (`inventory_step_function.asl.json`)**: Orchestrates the pipeline, coordinating validation, transformation, and notifications for success or failure.
- **Amazon S3**: Stores raw, validated, processed, rejected, and archived data, along with logs (`s3://misc-gtp-proj/logs/glue/inventory/`).
- **Amazon SNS**: Sends success/failure notifications to the topic `arn:aws:sns:us-east-1:985539772768:inventory_alert`.
- **Amazon CloudWatch**: Logs metrics and pipeline execution details for monitoring.

## Setup Instructions

### 1. Prerequisites
- **AWS Account**: Ensure access to an AWS account with permissions to manage Lambda, Glue, S3, Step Functions, SNS, and CloudWatch resources.
- **AWS CLI**: Install and configure the AWS CLI with credentials for programmatic access (`aws configure`).
- **Python 3.8+**: Required for local development and testing of Lambda and Glue scripts.
- **AWS SDK (boto3)**: Install via `pip install boto3`.
- **PySpark and Delta Lake**: Install dependencies for the transformation job:
  ```bash
  pip install pyspark delta-spark
  ```
- **S3 Bucket**: Create a bucket named `misc-gtp-proj` (or update the bucket name in scripts) with the following structure:
  - `landing_zone/raw/inventory/`: For raw JSONL files.
  - `landing_zone/validated/inventory/`: For validated Parquet files.
  - `landing_zone/processed/inventory/`: For Delta Lake tables.
  - `landing_zone/rejected/inventory/`: For rejected files.
  - `logs/glue/inventory/`: For validation and transformation logs.
  - `state/inventory/`: For state tracking (`state.json`).
- **Source Bucket**: Create a source bucket (`batch-data-source-v1`) with prefix `inventory/` for incoming JSONL files.
- **Archive Bucket**: Create an archive bucket (`batch-archive-grp2`) with prefix `inventory/` for archived raw files.
- **IAM Roles**:
  - **Lambda Execution Role**: Grant permissions for S3 (read/write), Step Functions (`states:StartExecution`), and CloudWatch Logs.
  - **Glue Execution Role**: Grant permissions for S3 (read/write), Glue, and CloudWatch Logs. Include Delta Lake dependencies in the Glue job configuration.
- **SNS Topic**: Create an SNS topic (`arn:aws:sns:us-east-1:985539772768:inventory_alert`) for notifications.
- **AWS Glue Setup**:
  - Ensure Glue is configured with Python 3 and the required libraries (`pyspark`, `delta-spark`).
  - Add the Delta Lake JAR to Glue job dependencies (e.g., `delta-core_2.12:2.4.0`).

### 2. Deploy AWS Lambda
1. **Package Lambda Code**:
   - Create a ZIP file containing `lambda_module.py` and dependencies:
     ```bash
     pip install boto3 -t lambda_package
     cp lambda_module.py lambda_package/
     cd lambda_package
     zip -r lambda_function.zip .
     ```
2. **Create Lambda Function**:
   - In the AWS Lambda console, create a function (`inventory_trigger`) with Python 3.8+.
   - Upload `lambda_function.zip`.
   - Set environment variables:
     - `STATE_MACHINE_ARN`: ARN of the Step Function (e.g., `arn:aws:states:us-east-1:985539772768:stateMachine:InventoryProcessing`).
     - `PROJECT_BUCKET`: `misc-gtp-proj`.
     - `LOG_FILE_PREFIX`: `logs/lambda/inventory`.
     - `BATCH_SOURCE_BUCKET`: `batch-data-source-v1`.
     - `INVENTORY_SOURCE_PREFIX`: `inventory/`.
   - Set the handler to `lambda_module.lambda_handler`.
   - Configure an SQS trigger (create an SQS queue for inventory file notifications) and set the batch size to 100.
   - Set timeout to 15 minutes and memory to 256 MB.

### 3. Deploy AWS Glue Jobs
1. **Validation Job (`inventory_validator`)**:
   - Upload `inventory_validate.py` to an S3 path (e.g., `s3://misc-gtp-proj/scripts/inventory_validate.py`).
   - In the AWS Glue console, create a Python Shell job:
     - Name: `inventory_validator`.
     - Script path: `s3://misc-gtp-proj/scripts/inventory_validate.py`.
     - Python version: 3.9.
     - IAM role: Glue execution role.
     - Add dependencies: `boto3`, `pandas`, `pyarrow`.
   - Set max concurrent runs to 1 and timeout to 60 minutes.
2. **Transformation Job (`inventory_transform`)**:
   - Upload `inventory_transform.py` to an S3 path (e.g., `s3://misc-gtp-proj/scripts/inventory_transform.py`).
   - Create a Spark job:
     - Name: `inventory_transform`.
     - Script path: `s3://misc-gtp-proj/scripts/inventory_transform.py`.
     - Spark version: Latest compatible with Delta Lake (e.g., Glue 4.0).
     - IAM role: Glue execution role.
     - Add dependencies: Delta Lake JAR (`delta-core_2.12:2.4.0`).
     - Configure Spark parameters as defined in `SPARK_CONFIGS` in `inventory_transform.py`.
   - Set max concurrent runs to 1 and timeout to 120 minutes.

### 4. Deploy AWS Step Function
1. **Upload State Machine Definition**:
   - Upload `inventory_step_function.asl.json` to an S3 path (e.g., `s3://misc-gtp-proj/state_machines/inventory_step_function.asl.json`).
2. **Create State Machine**:
   - In the AWS Step Functions console, create a state machine:
     - Name: `InventoryProcessing`.
     - Definition: Copy the contents of `inventory_step_function.asl.json`.
     - Type: Standard.
     - IAM role: Create or use a role with permissions for Glue (`glue:StartJobRun`), SNS (`sns:Publish`), and CloudWatch Logs.
   - Update the `TopicArn` in the `NotifySuccess` and `NotifyFailure` states to match your SNS topic ARN.
   - Update the `JobName` parameters in `RunInventoryValidatorJob` and `RunInventoryTransformJob` to match your Glue job names (`inventory_validator` and `inventory_transform`).

### 5. Configure SNS Topic
- Create an SNS topic named `inventory_alert` in the `us-east-1` region.
- Subscribe relevant endpoints (e.g., email, SMS, or Lambda) for success/failure notifications.
- Ensure the Step Function and Lambda roles have `sns:Publish` permissions for the topic.

### 6. Test the Pipeline
1. **Upload Sample Data**:
   - Create a sample JSONL file based on the inventory schema:
     ```json
     {"inventory_id": 1, "product_id": 101, "warehouse_id": 201, "stock_level": 100, "restock_threshold": 20, "last_updated": 1697059200.0}
     {"inventory_id": 2, "product_id": 102, "warehouse_id": 201, "stock_level": 50, "restock_threshold": 10, "last_updated": 1697059201.0}
     ```
   - Upload to `s3://batch-data-source-v1/inventory/sample.jsonl`.
2. **Trigger the Pipeline**:
   - Send an SQS message to the Lambda trigger queue with the file key:
     ```json
     {
       "Records": [
         {
           "body": "{\"Records\": [{\"s3\": {\"object\": {\"key\": \"inventory/sample.jsonl\"}}}]}"
         }
       ]
     }
     ```
   - Alternatively, configure S3 event notifications on `batch-data-source-v1/inventory/` to send to the SQS queue.
3. **Monitor Execution**:
   - Check Lambda logs in CloudWatch for file movement details.
   - Monitor Step Function executions for validation and transformation progress.
   - Verify Parquet files in `s3://misc-gtp-proj/landing_zone/validated/inventory/`.
   - Query the Delta Lake table in `s3://misc-gtp-proj/landing_zone/processed/inventory/` using AWS Glue Data Catalog or Athena.
   - Check SNS notifications for success/failure.

### 7. Security and Compliance
- **Data Encryption**: S3 buckets use server-side encryption (`AES256`) for data at rest. Ensure network traffic uses TLS.
- **RBAC**: Configure IAM roles with least privilege. Restrict access to S3 paths and Glue jobs based on roles.
- **Compliance**: The pipeline logs all operations to S3 and CloudWatch for auditability, supporting GDPR/CCPA requirements.
- **Data Quality**: Validation checks ensure schema compliance and business rules (e.g., non-negative stock levels, valid timestamps).

### 8. Monitoring and Governance
- **Pipeline Monitoring**: CloudWatch metrics track file processing counts and errors (`FilesProcessed`, `FilesSucceeded`, `FilesRejected`, `FilesErrored`).
- **Data Quality**: The validation job checks for missing columns, incorrect data types, and business rule violations.
- **Audit Logging**: Logs are written to `s3://misc-gtp-proj/logs/glue/inventory/` and CloudWatch for traceability.

## Usage
- **Input**: Place inventory JSONL files in `s3://batch-data-source-v1/inventory/`.
- **Output**: Validated data in `s3://misc-gtp-proj/landing_zone/validated/inventory/` (Parquet), processed data in `s3://misc-gtp-proj/landing_zone/processed/inventory/` (Delta Lake), and archived raw files in `s3://batch-archive-grp2/inventory/`.
- **Access**: Operations and Sales teams can query the Delta Lake table via SQL-based tools (e.g., Athena) for KPIs like inventory turnover and stockout alerts.

## Troubleshooting
- **Lambda Failures**: Check CloudWatch logs for `inventory_trigger` to diagnose S3 or Step Function errors.
- **Glue Job Failures**: Review CloudWatch logs for `inventory_validator` or `inventory_transform`. Verify input data schema and S3 permissions.
- **Step Function Failures**: Inspect execution history in the Step Functions console. Check SNS notifications for error details.
- **Data Issues**: Validate JSONL files against the schema in `Shopware V1.pdf`. Ensure timestamps are in epoch format and required fields are present.

## Maintenance
- **Scalability**: The pipeline uses adaptive partitioning in Spark and chunked file processing for large datasets.
- **Extensibility**: Add new validation rules in `inventory_validate.py` or transformations in `inventory_transform.py` as needed.
- **Updates**: Modify the Step Function ASL to include additional steps (e.g., for new data sources or processing tasks).
=======
# Web Analytics Pipeline

## Overview
This project implements a web analytics pipeline that processes weblogs, transforms data, and stores it for analysis using AWS services. The pipeline ingests web event data from an API, processes it through AWS Kinesis, stores it in S3, and makes it queryable via AWS Athena. The system calculates engagement and loyalty metrics for user sessions.

## Architecture
The pipeline consists of the following components:
- **API Ingestion**: A Python-based API consumer (`api.py`) polls a web traffic API and streams events to AWS Kinesis.
- **Data Transformation**: A Lambda function (`transformation.py`) processes Kinesis records, validates and cleans data, and writes partitioned Parquet files to S3.
- **Partition Registration**: Another Lambda function (`s3toathena.py`) registers new S3 partitions in AWS Athena for querying.
- **Analytics Queries**: SQL queries (`script.sql`) compute engagement and loyalty metrics from the processed data.
- **Containerization**: A Dockerized API service (`dockerfile-api`) runs on AWS ECS Fargate, defined by a task definition (`api_taskdefinition.json`).

## Components

### 1. API Consumer (`api.py`)
- **Purpose**: Polls a web traffic API, validates events, and streams them to Kinesis.
- **Key Features**:
  - Validates required fields (`session_id`, `user_id`, etc.).
  - Separates events with extra columns and stores them in a dedicated S3 folder.
  - Sends failed records to S3 (`failed_records`) and notifies via SNS.
  - Batches valid and extra-column events for efficient S3 uploads.
- **Environment Variables**:
  - `API_URL`: URL of the web traffic API.
  - `KINESIS_STREAM_NAME`: Kinesis stream for event streaming.
  - `S3_BUCKET`: S3 bucket for storing events.
  - `FAILED_RECORDS_SNS_ARN`: SNS topic for validation failure notifications.
  - `REGION`: AWS region (e.g., `us-east-1`).
  - `POLL_INTERVAL`: Polling interval in seconds (default: 1).
  - `S3_BATCH_SIZE`: Number of events to batch before S3 upload (default: 100).
  - `S3_BATCH_SECONDS`: Time interval for batch uploads (default: 10).

### 2. Transformation Lambda (`transformation.py`)
- **Purpose**: Processes Kinesis records, validates and transforms events, and writes partitioned Parquet files to S3.
- **Key Features**:
  - Validates required fields and generates anonymous `user_id` for missing values.
  - Converts epoch timestamps to datetime objects.
  - Partitions data by year, month, day, and hour.
  - Writes Parquet files with Snappy compression to S3.
  - Logs failed records to S3 and notifies via SNS.
- **Environment Variables**:
  - `S3_BUCKET`: Destination bucket for Parquet files.
  - `SNS_TOPIC_ARN`: SNS topic for failure notifications.
  - `REGION`: AWS region.

### 3. Athena Partition Registration (`s3toathena.py`)
- **Purpose**: Registers new S3 partitions in Athena to make data queryable.
- **Key Features**:
  - Triggered by S3 events for new objects.
  - Extracts partition information (year, month, day, hour) from S3 key paths.
  - Executes Athena DDL queries to add partitions.
  - Waits for query completion and logs status.
- **Environment Variables**:
  - `ATHENA_DATABASE`: Glue database name.
  - `ATHENA_TABLE`: Glue table name.
  - `S3_BUCKET`: Expected S3 bucket for events.
  - `ATHENA_OUTPUT`: S3 location for Athena query results.
  - `REGION`: AWS region.

### 4. Analytics Queries (`script.sql`)
- **Purpose**: Computes engagement and loyalty metrics from processed data.
- **Queries**:
  - **Engagement Metrics**: Calculates session duration, total events, unique pages, and entry/exit event types, grouped by session, user, device, and browser.
  - **Session Metrics**: Similar to engagement metrics, with additional comments for clarity.
  - **Loyalty Metrics**: Combines session data with CRM data to calculate engagement scores, loyalty interactions, loyalty rate percentage, and average ratings.
- **Parameters**:
  - `<<$startdate>>` and `<<$enddate>>`: Date range for filtering events.

### 5. ECS Task Definition (`api_taskdefinition.json`)
- **Purpose**: Defines the ECS Fargate task for running the API consumer.
- **Configuration**:
  - Uses Fargate with 512 CPU units and 1024 MB memory.
  - Specifies container image and environment variables.
  - Configures AWS CloudWatch Logs for logging.
- **Key Settings**:
  - `family`: `weblogs-api`
  - `networkMode`: `awsvpc`
  - `containerDefinitions`: Defines `api-container` with image and logging setup.

### 6. Dockerfile (`dockerfile-api`)
- **Purpose**: Builds the Docker image for the API consumer.
- **Details**:
  - Based on `python:3.9-slim`.
  - Installs dependencies from `requirements.txt`.
  - Copies and runs `api.py` as the entry point.

## Setup Instructions
1. **Prerequisites**:
   - AWS account with permissions for ECS, Kinesis, S3, Athena, SNS, and Lambda.
   - Docker installed for building the API container.
   - Python 3.9+ for local testing.
   - AWS CLI configured with appropriate credentials.

2. **Build and Deploy API Container**:
   - Build the Docker image:
     ```
     docker build -t weblogs-api -f dockerfile-api .
     ```
   - Push to Amazon ECR:
     ```
     aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 985539772768.dkr.ecr.us-east-1.amazonaws.com
     docker tag weblogs-api:latest 985539772768.dkr.ecr.us-east-1.amazonaws.com/weblogs/api-gtp:api
     docker push 985539772768.dkr.ecr.us-east-1.amazonaws.com/weblogs/api-gtp:api
     ```

3. **Deploy ECS Task**:
   - Update `api_taskdefinition.json` with your ECR image URI and environment variables.
   - Register the task definition:
     ```
     aws ecs register-task-definition --cli-input-json file://api_taskdefinition.json
     ```
   - Create or update an ECS service to run the task.

4. **Set Up S3 and Athena**:
   - Create an S3 bucket (`weblogs-bucket-gtp`) for storing events.
   - Set up an Athena database and table with partitions (year, month, day, hour).
   - Configure `athena_output` S3 prefix for query results.

5. **Deploy Lambda Functions**:
   - Package and deploy `transformation.py` and `s3toathena.py` as Lambda functions.
   - Configure triggers:
     - For `transformation.py`: Kinesis stream (`weblogs_stream`).
     - For `s3toathena.py`: S3 bucket events for the `events/` prefix.
   - Set environment variables as specified.

6. **Run Analytics Queries**:
   - Use `script.sql` in Athena to query engagement and loyalty metrics.
   - Replace `<<$startdate>>` and `<<$enddate>>` with desired date ranges.

## Usage
- The pipeline runs continuously, polling the API and processing events.
- Query Athena for analytics results using `script.sql`.
- Monitor CloudWatch Logs for errors and SNS notifications for failed records.
- Check S3 buckets for valid, failed, and extra-column records.

## Error Handling
- **API Consumer**: Logs errors, stores failed records in S3 (`failed_records`, `kinesis_failures`), and sends SNS notifications.
- **Transformation Lambda**: Logs invalid events, generates anonymous `user_id` for missing values, and stores failed records in S3 (`failed_transformation_records`).
- **Athena Registration**: Logs query failures and skips invalid S3 keys.

## Monitoring
- Use CloudWatch Logs for ECS and Lambda logs.
- Monitor SNS topics for failure notifications.
- Check S3 bucket for stored events and failed records.

## Limitations
- Assumes API returns JSON data with expected fields.
- Timestamp conversion in `transformation.py` expects epoch format (seconds or milliseconds).
- Partitioning assumes standard year/month/day/hour structure.
- No support for non-JSON API responses.

## Future Improvements
- Add data validation for event types and other fields.
- Implement retry logic for Athena query failures.
- Add metrics for pipeline throughput and latency.
- Support additional data formats or schema evolution.
