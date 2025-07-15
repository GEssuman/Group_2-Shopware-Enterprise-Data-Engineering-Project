import os
import json
import time
import logging
import requests
import boto3
from botocore.exceptions import BotoCoreError, ClientError
from requests.exceptions import RequestException
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configuration from environment or defaults
API_URL = os.getenv("API_URL", "http://4.328.189.26:8000/api/-traffic/")
KINESIS_STREAM_NAME = os.getenv("KINESIS_STREAM_NAME")
REGION = os.getenv("AWS_REGION", "us-east-1")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", 60))
MAX_RETRIES = 5

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Initialize Kinesis client
kinesis = boto3.client("kinesis", region_name=REGION)

def fetch_traffic_data():
    retries = 0
    while retries < MAX_RETRIES:
        try:
            response = requests.get(API_URL, timeout=10)
            response.raise_for_status()
            return response.json()
        except RequestException as e:
            wait = 2 ** retries
            logging.warning(f"API request failed (attempt {retries + 1}): {e}. Retrying in {wait} seconds...")
            time.sleep(wait)
            retries += 1
    logging.error("Exceeded max retries for API request.")
    return None

def send_to_kinesis(data):
    try:
        # Convert data to JSON string and encode as bytes
        payload = json.dumps(data)
        response = kinesis.put_record(
            StreamName=KINESIS_STREAM_NAME,
            Data=payload,
            PartitionKey=data.get("session_id", "default-key")
        )
        logging.info(f"Sent record to Kinesis: ShardId={response['ShardId']}, SequenceNumber={response['SequenceNumber']}")
    except (BotoCoreError, ClientError) as e:
        logging.error(f"Failed to send data to Kinesis: {e}")

def main_loop():
    if not KINESIS_STREAM_NAME:
        logging.error("KINESIS_STREAM_NAME environment variable is not set. Exiting.")
        return

    logging.info(f"Starting data fetch loop. Poll interval: {POLL_INTERVAL} seconds.")
    while True:
        data = fetch_traffic_data()
        if data:
            logging.info("Fetched data:")
            logging.info(json.dumps(data, indent=2))

            # Optional: append to local backup file
            with open("traffic_data.json", "a") as f:
                json.dump(data, f)
                f.write("\n")

            # Send data to Kinesis
            send_to_kinesis(data)

        logging.info(f"Sleeping for {POLL_INTERVAL} seconds...\n")
        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    main_loop()
