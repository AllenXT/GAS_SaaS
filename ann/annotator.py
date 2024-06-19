import boto3
import os
import subprocess
import json
from botocore.exceptions import ClientError
import configparser
import helpers

# Load configuration from ann_config.ini
config = configparser.ConfigParser()
config.read('ann_config.ini')

AWS_REGION = config.get('AWS', 'REGION')
SQS_URL = config.get('SQS', 'REQUESTS_URL')
DDB_TABLE_NAME = config.get('DDB', 'TABLE_NAME')
INPUT_BUCKET = config.get('S3', 'INPUT_BUCKET')

# current directory
# https://www.geeksforgeeks.org/get-current-directory-python/
CUR_DIR = os.path.dirname(os.path.realpath(__file__))

# set up the AWS resources
s3_client = boto3.client('s3', region_name=AWS_REGION)
dynamoDB = boto3.resource('dynamodb', region_name=AWS_REGION)
sqs_client = boto3.client('sqs', region_name=AWS_REGION)
table = dynamoDB.Table(DDB_TABLE_NAME)

def update_job_status(job_id):
    try:
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/table/update_item.html
        response = table.update_item(
            Key={'job_id': job_id},
            UpdateExpression='SET job_status = :status',
            ConditionExpression='job_status = :expected_status',
            ExpressionAttributeValues={
                ':status': 'RUNNING',
                ':expected_status': 'PENDING'
            }
        )
    except ClientError as e:
        print(f"Failed to update DynamoDB item status for job {job_id}: {e}")
        raise

while True:
    try:
        # Poll the message queue in a loop
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs/client/receive_message.html
        response = sqs_client.receive_message(
            QueueUrl=SQS_URL,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=5
        )
    except ClientError as e:
        print(f"SQS Failed to receive messages: {e}")
        continue

    messages = response.get('Messages', [])
    if not messages:
        continue  # keep polling if no message is received

    message = messages[0]
    # parse the message body
    receipt_handle = message['ReceiptHandle']
    body = json.loads(message['Body'])
    msg = json.loads(body['Message'])

    # Extract job parameters from the message body
    job_id = msg['job_id']
    key = msg['s3_key_input_file']
    user_id = msg['user_id']
    
    # use helper function to get user name and email
    # user_profile = helpers.get_user_profile(id=user_id)
    user_name = msg['user_name']
    user_email = msg['user_email']

    # create a working directory for annotation jobs
    job_dir = os.path.join(CUR_DIR, job_id)
    os.makedirs(job_dir, exist_ok=True)

    file_path = os.path.join(job_dir, os.path.basename(key))
    # download the input file from S3
    try:
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/download_file.html
        s3_client.download_file(INPUT_BUCKET, key, file_path)
    except ClientError as e:
        print(f"Failed to download file from S3 for job {job_id}: {e}")
        continue
    
    # run the annotation job in a subprocess
    command = ['python', 'run.py', f'{job_id}/{os.path.basename(key)}', job_id, user_id, user_name, user_email]
    try:
        # https://stackoverflow.com/questions/12605498/how-to-use-subprocess-popen-python
        subprocess.Popen(command, cwd=CUR_DIR)
        # update DynamoDB item to RUNNING
        update_job_status(job_id)
        print(f"Job {job_id} started with input file {os.path.basename(key).split('~')[-1]}.")
    except Exception as e:
        print(f"Failed to start job {job_id}: {e}")
    else:
        # If Popen succeeds, then attempt to delete the message
        try:
            # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs/client/delete_message.html
            sqs_client.delete_message(
                QueueUrl=SQS_URL,
                ReceiptHandle=receipt_handle
            )
            print(f"Message for job {job_id} deleted in SQS.")
        except Exception as e:
            print(f"Failed to delete message for job {job_id} in SQS: {e}")