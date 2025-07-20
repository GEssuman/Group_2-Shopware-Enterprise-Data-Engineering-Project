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