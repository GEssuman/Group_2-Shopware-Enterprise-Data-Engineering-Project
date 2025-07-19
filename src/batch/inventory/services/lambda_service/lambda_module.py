import json
import os
import boto3
import logging
from datetime import datetime
from botocore.exceptions import ClientError, BotoCoreError
from typing import Dict, List, Any, Optional, Tuple
import uuid
from urllib.parse import urlparse
import botocore
# Set up logging with structured format
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Create a custom formatter for better log structure
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Initialize AWS clients outside the handler for performance
# Use session for better connection management
session = boto3.Session()
sfn_client = session.client('stepfunctions')
s3_client = session.client('s3')

# Configuration with validation
STATE_MACHINE_ARN = os.environ.get('STATE_MACHINE_ARN')
S3_BUCKET = os.environ.get('PROJECT_BUCKET', 'misc-gtp-proj')
LOG_FILE_PREFIX = os.environ.get('LOG_FILE_PREFIX')
BATCH_SOURCE_BUCKET = os.environ.get('BATCH_SOURCE_BUCKET', 'batch-data-source-v1')
INVENTORY_SOURCE_PREFIX = os.environ.get('INVENTORY_SOURCE_PREFIX', 'inventory/')
S3_RAW_PATH = f"s3://{S3_BUCKET}/landing_zone/raw"


STATE_FILE_BUCKET = 'misc-gtp-proj'
STATE_FILE_KEY = 'state/inventory/state.json'

MAX_BATCH_SIZE = 100
MAX_RETRIES = 3
MAX_CHUNK_SIZE = 50 * 1024  # 50KB per chunk default
MAX_MESSAGES_PER_CHUNK = 50  # Max messages per chunk

# Validate required environment variables
if not STATE_MACHINE_ARN:
    raise ValueError("STATE_MACHINE_ARN environment variable is required")

def write_log_to_s3(log_message: str, log_level: str = 'INFO') -> bool:
    """
    Writes a log message to a file in S3, named by date.
    
    Args:
        log_message: The message to log
        log_level: The log level (INFO, WARNING, ERROR)
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Create a log file name based on the current date
        current_date = datetime.utcnow().strftime('%Y-%m-%d')
        log_file_key = f"{LOG_FILE_PREFIX}-{current_date}.log"
        
        # Format the log message with a timestamp and level
        timestamp = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')
        formatted_message = f"[{timestamp}] [{log_level}] {log_message}\n"
        
        # Use a more efficient approach for appending to S3
        # Instead of reading the entire file, we'll use S3's append-like behavior
        # by creating unique keys for each log entry and using S3 Select for reading
        
        # For now, we'll use the original approach but with better error handling
        existing_content = ""
        try:
            response = s3_client.get_object(Bucket=S3_BUCKET, Key=log_file_key)
            existing_content = response['Body'].read().decode('utf-8')
        except ClientError as e:
            if e.response['Error']['Code'] != 'NoSuchKey':
                raise  # Re-raise if it's not a "file doesn't exist" error
        
        new_content = existing_content + formatted_message
        
        # Write with explicit content type and server-side encryption
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=log_file_key,
            Body=new_content.encode('utf-8'),
            ContentType='text/plain',
            ServerSideEncryption='AES256'
        )
        return True
        
    except (ClientError, BotoCoreError) as e:
        logger.error(f"AWS error writing log to S3: {str(e)}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error writing log to S3: {str(e)}")
        return False
    


def state_file_exists() -> bool:
    try:
        s3_client.head_object(Bucket=STATE_FILE_BUCKET, Key=STATE_FILE_KEY)
        return True
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == '404':
            return False
        else:
            raise

def write_state_file(state_data: Dict[str, Any]) -> None:
    s3_client.put_object(
        Bucket=STATE_FILE_BUCKET,
        Key=STATE_FILE_KEY,
        Body=json.dumps(state_data).encode('utf-8'),
        ContentType='application/json',
        ServerSideEncryption='AES256'
    )


def list_inventory_files(correlation_id: str, prefix: str = None) -> List[Dict[str, Any]]:
    """
    List all files in the inventory source S3 location.
    
    Args:
        correlation_id: Unique identifier for tracking
        prefix: Optional prefix to filter files
    
    Returns:
        List of file metadata dictionaries
    """
    try:
        files = []
        paginator = s3_client.get_paginator('list_objects_v2')
        
        list_prefix = INVENTORY_SOURCE_PREFIX
        if prefix:
            list_prefix = f"{INVENTORY_SOURCE_PREFIX}{prefix}"
        
        page_iterator = paginator.paginate(
            Bucket=BATCH_SOURCE_BUCKET,
            Prefix=list_prefix
        )
        
        for page in page_iterator:
            if 'Contents' in page:
                for obj in page['Contents']:
                    # Skip directories (keys ending with /)
                    if not obj['Key'].endswith('/'):
                        files.append({
                            'key': obj['Key'],
                            'size': obj['Size'],
                            'last_modified': obj['LastModified'].isoformat(),
                            'etag': obj['ETag'].strip('"')
                        })
        
        log_message = f"[{correlation_id}] Found {len(files)} files in source location"
        logger.info(log_message)
        write_log_to_s3(log_message, 'INFO')
        
        return files
        
    except ClientError as e:
        log_message = f"[{correlation_id}] Error listing inventory files: {str(e)}"
        logger.error(log_message)
        write_log_to_s3(log_message, 'ERROR')
        raise e
    except Exception as e:
        log_message = f"[{correlation_id}] Unexpected error listing inventory files: {str(e)}"
        logger.error(log_message)
        write_log_to_s3(log_message, 'ERROR')
        raise e


def extract_s3_keys_from_event(event: Dict[str, Any]) -> List[str]:
    keys = []
    for record in event.get('Records', []):
        body = record.get('body')
        if body:
            try:
                message = json.loads(body)
                for rec in message.get('Records', []):
                    key = rec.get('s3', {}).get('object', {}).get('key')
                    if key:
                        keys.append(key)
            except Exception as ex:
                logger.warning(f"Failed to parse SQS message body for keys: {ex}")
    return list(set(keys))  # deduplicate


def move_files_according_to_state_and_event(correlation_id: str, event: Dict[str, Any]) -> Dict[str, Any]:
    if state_file_exists():
        keys_to_move = extract_s3_keys_from_event(event)
        if not keys_to_move:
            logger.info(f"[{correlation_id}] State file exists but no keys found in event; nothing to move.")
            return {
                'total_files': 0,
                'successful_moves': 0,
                'failed_moves': 0,
                'moved_files': [],
                'failed_files': []
            }
        all_source_files = list_inventory_files(correlation_id)
        files_to_move = [f for f in all_source_files if f['key'] in keys_to_move]
    else:
        files_to_move = list_inventory_files(correlation_id)

    moved_files = []
    failed_files = []

    for file_info in files_to_move:
        src_key = file_info['key']
        success, dest_key = move_file_to_raw_zone(src_key, correlation_id)
        if success:
            moved_files.append({
                'source_key': src_key,
                'destination_key': dest_key,
                'size': file_info['size'],
                'moved_at': datetime.utcnow().isoformat()
            })
        else:
            failed_files.append({
                'source_key': src_key,
                'error': 'Failed to move file',
                'size': file_info['size']
            })

    state_content = {
        'last_updated': datetime.utcnow().isoformat(),
        'moved_files': moved_files
    }
    write_state_file(state_content)

    log_message = (
        f"[{correlation_id}] File movement completed: "
        f"{len(moved_files)} successful, {len(failed_files)} failed"
    )
    logger.info(log_message)
    write_log_to_s3(log_message, 'INFO')

    return {
        'total_files': len(files_to_move),
        'successful_moves': len(moved_files),
        'failed_moves': len(failed_files),
        'moved_files': moved_files,
        'failed_files': failed_files
    }




def move_file_to_raw_zone(source_key: str, correlation_id: str) -> Tuple[bool, str]:
    """
    Move a file from the source bucket to the raw landing zone.
    
    Args:
        source_key: The S3 key of the source file
        correlation_id: Unique identifier for tracking
    
    Returns:
        Tuple of (success: bool, destination_key: str)
    """
    try:
        # Generate destination key with timestamp for uniqueness
        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        filename = os.path.basename(source_key)
        destination_key = f"landing_zone/raw/inventory/{timestamp}_{filename}"
        
        # Copy the file to the destination
        copy_source = {
            'Bucket': BATCH_SOURCE_BUCKET,
            'Key': source_key
        }
        
        s3_client.copy_object(
            CopySource=copy_source,
            Bucket=S3_BUCKET,
            Key=destination_key,
            ServerSideEncryption='AES256',
            MetadataDirective='COPY'
        )
        
        # Verify the copy was successful by checking if the file exists
        try:
            s3_client.head_object(Bucket=S3_BUCKET, Key=destination_key)
        except ClientError:
            log_message = f"[{correlation_id}] Failed to verify copied file: {destination_key}"
            logger.error(log_message)
            write_log_to_s3(log_message, 'ERROR')
            return False, destination_key
        
        # Delete the source file after successful copy
        #s3_client.delete_object(Bucket=BATCH_SOURCE_BUCKET, Key=source_key)
        
        log_message = f"[{correlation_id}] Successfully moved file: {source_key} -> {destination_key}"
        logger.info(log_message)
        write_log_to_s3(log_message, 'INFO')
        
        return True, destination_key
        
    except ClientError as e:
        log_message = f"[{correlation_id}] AWS error moving file {source_key}: {str(e)}"
        logger.error(log_message)
        write_log_to_s3(log_message, 'ERROR')
        return False, ""
    except Exception as e:
        log_message = f"[{correlation_id}] Unexpected error moving file {source_key}: {str(e)}"
        logger.error(log_message)
        write_log_to_s3(log_message, 'ERROR')
        return False, ""

def process_file_movement(correlation_id: str, file_filter: str = None) -> Dict[str, Any]:
    """
    Process the movement of inventory files from source to raw landing zone.
    
    Args:
        correlation_id: Unique identifier for tracking
        file_filter: Optional filter for file processing (e.g., file extension)
    
    Returns:
        Dictionary with movement results
    """
    try:
        # List all files in the source location
        source_files = list_inventory_files(correlation_id, file_filter)
        
        if not source_files:
            log_message = f"[{correlation_id}] No files found to process"
            logger.info(log_message)
            write_log_to_s3(log_message, 'INFO')
            return {
                'total_files': 0,
                'successful_moves': 0,
                'failed_moves': 0,
                'moved_files': [],
                'failed_files': []
            }
        
        # Process each file
        moved_files = []
        failed_files = []
        
        for file_info in source_files:
            source_key = file_info['key']
            success, destination_key = move_file_to_raw_zone(source_key, correlation_id)
            
            if success:
                moved_files.append({
                    'source_key': source_key,
                    'destination_key': destination_key,
                    'size': file_info['size'],
                    'moved_at': datetime.utcnow().isoformat()
                })
            else:
                failed_files.append({
                    'source_key': source_key,
                    'error': 'Failed to move file',
                    'size': file_info['size']
                })
        
        results = {
            'total_files': len(source_files),
            'successful_moves': len(moved_files),
            'failed_moves': len(failed_files),
            'moved_files': moved_files,
            'failed_files': failed_files
        }
        
        log_message = f"[{correlation_id}] File movement completed: {len(moved_files)} successful, {len(failed_files)} failed"
        logger.info(log_message)
        write_log_to_s3(log_message, 'INFO')
        
        return results
        
    except Exception as e:
        log_message = f"[{correlation_id}] Error in file movement process: {str(e)}"
        logger.error(log_message)
        write_log_to_s3(log_message, 'ERROR')
        raise e

def parse_sqs_messages(records: List[Dict[str, Any]]) -> tuple[List[Dict[str, Any]], List[str]]:
    """
    Parse SQS messages and separate valid from invalid ones.
    
    Args:
        records: List of SQS record dictionaries
    
    Returns:
        tuple: (valid_messages, error_messages)
    """
    valid_messages = []
    error_messages = []
    
    for i, record in enumerate(records):
        try:
            message_body = record.get('body')
            if not message_body:
                error_msg = f"Record {i} has empty body"
                error_messages.append(error_msg)
                continue
            
            # Validate that it's valid JSON
            try:
                data = json.loads(message_body)
                if not isinstance(data, dict):
                    error_msg = f"Record {i} body is not a JSON object"
                    error_messages.append(error_msg)
                    continue
                
                # Add message metadata for tracking
                data['_message_id'] = record.get('messageId', str(uuid.uuid4()))
                data['_receipt_handle'] = record.get('receiptHandle')
                data['_approximate_receive_count'] = record.get('attributes', {}).get('ApproximateReceiveCount', '1')
                
                valid_messages.append(data)
                
            except json.JSONDecodeError as e:
                error_msg = f"Record {i} contains invalid JSON: {str(e)}"
                error_messages.append(error_msg)
                continue
                
        except Exception as e:
            error_msg = f"Unexpected error processing record {i}: {str(e)}"
            error_messages.append(error_msg)
    
    return valid_messages, error_messages

def chunk_messages(messages: List[Dict[str, Any]], max_chunk_size: int = None) -> List[List[Dict[str, Any]]]:
    """
    Chunk messages into smaller batches based on size and count limits.
    
    Args:
        messages: List of messages to chunk
        max_chunk_size: Maximum size per chunk in bytes (defaults to MAX_CHUNK_SIZE)
    
    Returns:
        List of message chunks
    """
    if max_chunk_size is None:
        max_chunk_size = MAX_CHUNK_SIZE
    
    chunks = []
    current_chunk = []
    current_size = 0
    
    # Base payload overhead for size calculation
    base_payload = {
        'correlation_id': 'sample-correlation-id',
        'timestamp': datetime.utcnow().isoformat(),
        'processing_mode': 'chunked',
        'total_chunks': 1,
        'total_messages': len(messages),
        'parsing_errors': [],
        'chunks': [{'chunk_number': 1, 'chunk_size': 0, 'inventory_updates': []}]
    }
    base_size = len(json.dumps(base_payload).encode('utf-8'))
    
    for message in messages:
        message_size = len(json.dumps(message).encode('utf-8'))
        
        # Check if adding this message would exceed size or count limits
        would_exceed_size = current_chunk and (current_size + message_size + base_size) > max_chunk_size
        would_exceed_count = len(current_chunk) >= MAX_MESSAGES_PER_CHUNK
        
        if would_exceed_size or would_exceed_count:
            # Start a new chunk
            if current_chunk:  # Only add non-empty chunks
                chunks.append(current_chunk)
            current_chunk = [message]
            current_size = message_size
        else:
            current_chunk.append(message)
            current_size += message_size
    
    # Add the last chunk if it has messages
    if current_chunk:
        chunks.append(current_chunk)
    
    return chunks

def validate_chunk_size(chunk: List[Dict[str, Any]], max_size: int = None) -> bool:
    """
    Validate that a chunk size doesn't exceed Step Functions limits.
    
    Args:
        chunk: The chunk to validate
        max_size: Maximum size in bytes
    
    Returns:
        bool: True if valid, False otherwise
    """
    if max_size is None:
        max_size = 200 * 1024  # 200KB
    
    try:
        # Create a sample payload to test size
        test_payload = {
            'correlation_id': 'test-correlation-id',
            'timestamp': datetime.utcnow().isoformat(),
            'inventory_updates': chunk,
            'batch_size': len(chunk),
            'parsing_errors': [],
            'chunk_info': {
                'chunk_number': 1,
                'total_chunks': 1,
                'total_messages': len(chunk)
            }
        }
        
        payload_size = len(json.dumps(test_payload).encode('utf-8'))
        
        if payload_size > max_size:
            logger.warning(f"Chunk size ({payload_size} bytes) exceeds limit ({max_size} bytes)")
            return False
        
        return True
    except Exception as e:
        logger.error(f"Error validating chunk size: {str(e)}")
        return False

def start_single_step_function_execution(payload: Dict[str, Any]) -> Optional[str]:
    """
    Start Step Function execution with a single payload.
    
    Args:
        payload: The payload to send to Step Function
    
    Returns:
        str: Execution ARN if successful, None otherwise
    """
    for attempt in range(MAX_RETRIES):
        try:
            execution_name = f"inventory-single-{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:8]}"
            
            response = sfn_client.start_execution(
                stateMachineArn=STATE_MACHINE_ARN,
                name=execution_name,
                input=json.dumps(payload)
            )
            
            return response['executionArn']
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'ExecutionLimitExceeded':
                logger.warning(f"Step Function execution limit exceeded, attempt {attempt + 1}")
                if attempt < MAX_RETRIES - 1:
                    continue
            else:
                logger.error(f"ClientError starting Step Function (attempt {attempt + 1}): {str(e)}")
                if attempt < MAX_RETRIES - 1:
                    continue
        except Exception as e:
            logger.error(f"Unexpected error starting Step Function (attempt {attempt + 1}): {str(e)}")
            if attempt < MAX_RETRIES - 1:
                continue
    
    return None

def start_step_function_with_chunks(chunks: List[List[Dict[str, Any]]], 
                                   correlation_id: str, 
                                   error_messages: List[str],
                                   file_movement_results: Dict[str, Any] = None,
                                   input_path: Optional[str] = None) -> Optional[str]:
    """
    Start Step Function execution with chunked messages sent sequentially.
    
    Args:
        chunks: List of message chunks
        correlation_id: Unique identifier for this batch
        error_messages: List of parsing errors
        file_movement_results: Results from file movement operation
    
    Returns:
        str: Execution ARN if successful, None otherwise
    """
    total_chunks = len(chunks)
    total_messages = sum(len(chunk) for chunk in chunks)
    
    # Create the main payload that will orchestrate chunk processing
    main_payload = {
        'correlation_id': correlation_id,
        'timestamp': datetime.utcnow().isoformat(),
        'processing_mode': 'chunked',
        'total_chunks': total_chunks,
        'total_messages': total_messages,
        'parsing_errors': error_messages,
        'file_movement_results': file_movement_results or {},
        'input_path': input_path,
        'chunks': []
    }
    
    # Add each chunk with metadata
    for i, chunk in enumerate(chunks):
        chunk_payload = {
            'chunk_number': i + 1,
            'chunk_size': len(chunk),
            'inventory_updates': chunk
        }
        main_payload['chunks'].append(chunk_payload)
    
    # Validate the complete payload size
    payload_size = len(json.dumps(main_payload).encode('utf-8'))
    max_size = 200 * 1024  # 200KB
    
    if payload_size > max_size:
        logger.error(f"Complete chunked payload ({payload_size} bytes) still exceeds limit ({max_size} bytes)")
        return None
    
    # Start the Step Function execution
    for attempt in range(MAX_RETRIES):
        try:
            execution_name = f"inventory-chunked-{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:8]}"
            
            response = sfn_client.start_execution(
                stateMachineArn=STATE_MACHINE_ARN,
                name=execution_name,
                input=json.dumps(main_payload)
            )
            
            return response['executionArn']
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            logger.error(f"ClientError starting chunked Step Function (attempt {attempt + 1}): {str(e)}")
            if attempt < MAX_RETRIES - 1 and error_code == 'ExecutionLimitExceeded':
                continue
        except Exception as e:
            logger.error(f"Unexpected error starting chunked Step Function (attempt {attempt + 1}): {str(e)}")
            if attempt < MAX_RETRIES - 1:
                continue
    
    return None

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Handles incoming messages from an SQS queue, processes file movement, and triggers Step Function execution.
    
    This function:
    1. Processes a batch of SQS messages
    2. Moves inventory files from source to raw landing zone
    3. Validates and extracts message content
    4. Triggers an AWS Step Function execution with the processed data
    
    Args:
        event: The event dictionary from the SQS trigger
        context: The Lambda context object
    
    Returns:
        dict: Response with statusCode and body
    
    Raises:
        Exception: If critical errors occur that should trigger SQS retry
    """
    # Extract correlation ID for request tracking
    correlation_id = str(uuid.uuid4())
    
    # Log function start
    records = event.get('Records', [])
    log_message = f"[{correlation_id}] Lambda started - Processing {len(records)} SQS messages"
    logger.info(log_message)
    write_log_to_s3(log_message, 'INFO')

    # Define the S3 input path for the Step Function
    # It represents the base S3 location where raw inventory files are moved.
    s3_input_path = S3_RAW_PATH
    
    # Initialize file movement results
    file_movement_results = {}
    
    try:
        # Step 1: Process file movement from source to raw landing zone
        log_message = f"[{correlation_id}] Starting file movement process"
        logger.info(log_message)
        write_log_to_s3(log_message, 'INFO')
        
        file_movement_results = move_files_according_to_state_and_event(correlation_id, event)
        
        # Log file movement summary
        log_message = f"[{correlation_id}] File movement completed - {file_movement_results.get('successful_moves', 0)} successful, {file_movement_results.get('failed_moves', 0)} failed"
        logger.info(log_message)
        write_log_to_s3(log_message, 'INFO')
        
    except Exception as e:
        log_message = f"[{correlation_id}] Error in file movement process: {str(e)}"
        logger.error(log_message)
        write_log_to_s3(log_message, 'ERROR')
        # Continue processing even if file movement fails
        file_movement_results = {
            'total_files': 0,
            'successful_moves': 0,
            'failed_moves': 0,
            'moved_files': [],
            'failed_files': [],
            'error': str(e)
        }
    
    # Step 2: Validate SQS input
    if not records:
        log_message = f"[{correlation_id}] No records found in event"
        logger.info(log_message)
        write_log_to_s3(log_message, 'INFO')
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'No records to process',
                'file_movement_results': file_movement_results
            })
        }
    
    # Check batch size
    if len(records) > MAX_BATCH_SIZE:
        log_message = f"[{correlation_id}] Batch size ({len(records)}) exceeds maximum ({MAX_BATCH_SIZE})"
        logger.warning(log_message)
        write_log_to_s3(log_message, 'WARNING')
        # Process only the first MAX_BATCH_SIZE records
        records = records[:MAX_BATCH_SIZE]
    
    # Step 3: Parse SQS messages
    try:
        valid_messages, error_messages = parse_sqs_messages(records)
        
        # Log parsing results
        if error_messages:
            for error_msg in error_messages:
                log_message = f"[{correlation_id}] Parse error: {error_msg}"
                logger.warning(log_message)
                write_log_to_s3(log_message, 'WARNING')
        
        if not valid_messages:
            log_message = f"[{correlation_id}] No valid messages found after parsing"
            logger.info(log_message)
            write_log_to_s3(log_message, 'INFO')
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'No valid messages found',
                    'errors': error_messages,
                    'file_movement_results': file_movement_results
                })
            }
        
        # Step 4: Determine processing approach based on payload size
        total_payload_size = len(json.dumps({'inventory_updates': valid_messages}).encode('utf-8'))
        max_single_payload_size = 180 * 1024  # 180KB to leave buffer
        
        if total_payload_size <= max_single_payload_size:
            # Single payload approach
            step_function_payload = {
                'correlation_id': correlation_id,
                'timestamp': datetime.utcnow().isoformat(),
                'processing_mode': 'single',
                'inventory_updates': valid_messages,
                'batch_size': len(valid_messages),
                'parsing_errors': error_messages,
                'file_movement_results': file_movement_results,
                'input_path': s3_input_path
            }
            
            # Start Step Function execution
            log_message = f"[{correlation_id}] Starting Step Function with single payload ({len(valid_messages)} updates)"
            logger.info(log_message)
            write_log_to_s3(log_message, 'INFO')
            
            execution_arn = start_single_step_function_execution(step_function_payload)
            
        else:
            # Chunked approach
            log_message = f"[{correlation_id}] Payload too large ({total_payload_size} bytes), using chunked approach"
            logger.info(log_message)
            write_log_to_s3(log_message, 'INFO')
            
            # Chunk the messages
            chunks = chunk_messages(valid_messages)
            
            # Validate each chunk
            valid_chunks = []
            for i, chunk in enumerate(chunks):
                if validate_chunk_size(chunk):
                    valid_chunks.append(chunk)
                else:
                    log_message = f"[{correlation_id}] Chunk {i+1} is too large, skipping {len(chunk)} messages"
                    logger.warning(log_message)
                    write_log_to_s3(log_message, 'WARNING')
            
            if not valid_chunks:
                log_message = f"[{correlation_id}] No valid chunks after size validation"
                logger.error(log_message)
                write_log_to_s3(log_message, 'ERROR')
                raise Exception("All chunks exceed size limits")
            
            log_message = f"[{correlation_id}] Starting Step Function with {len(valid_chunks)} chunks"
            logger.info(log_message)
            write_log_to_s3(log_message, 'INFO')
            
            execution_arn = start_step_function_with_chunks(valid_chunks, correlation_id, error_messages, file_movement_results, s3_input_path)
        
        if execution_arn:
            log_message = f"[{correlation_id}] Step Function started successfully: {execution_arn}"
            logger.info(log_message)
            write_log_to_s3(log_message, 'INFO')
            
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'Step Function started successfully',
                    'executionArn': execution_arn,
                    'correlation_id': correlation_id,
                    'processed_count': len(valid_messages),
                    'error_count': len(error_messages),
                    'processing_mode': 'chunked' if total_payload_size > max_single_payload_size else 'single',
                    'file_movement_results': file_movement_results
                })
            }
        else:
            log_message = f"[{correlation_id}] Failed to start Step Function after {MAX_RETRIES} attempts"
            logger.error(log_message)
            write_log_to_s3(log_message, 'ERROR')
            raise Exception("Failed to start Step Function execution")
            
    except Exception as e:
        log_message = f"[{correlation_id}] Critical error in lambda_handler: {str(e)}"
        logger.error(log_message)
        write_log_to_s3(log_message, 'ERROR')
        # Re-raise to trigger SQS retry mechanism
        raise e