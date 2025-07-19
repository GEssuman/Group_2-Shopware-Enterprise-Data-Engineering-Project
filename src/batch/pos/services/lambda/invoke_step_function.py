import boto3
from datetime import datetime
import os
import json

step_function_arn = os.getenv("POS_STEP_FUNCTION_ARN")

stepfunctions = boto3.client('stepfunctions')

def lambda_handler(event, context):
    current_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")  # e.g., 20250717_084534

    input_payload = {
        "event_time": current_timestamp
    }

    response = stepfunctions.start_execution(
        stateMachineArn=step_function_arn,
        input=json.dumps(input_payload) 
    )

    return {
        "statusCode": 200,
        "body": f"Step Function started with event_time: {current_timestamp}",
        "executionArn": response["executionArn"]
    }
