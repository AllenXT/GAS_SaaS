import os
import sys
import boto3
import json
from datetime import datetime, timedelta

# Import utility helpers
sys.path.insert(1, os.path.realpath(os.path.pardir))
import helpers

# Get configuration
from configparser import SafeConfigParser
config = SafeConfigParser(os.environ)
config.read('archive_config.ini')

REGION_NAME = config.get('AWS', 'REGION')
SQS_URL = config.get('SQS', 'ARCHIVE_URL')
GLACIER_VAULT = config.get('GLACIER', 'VAULT')
TABLE_NAME = config.get('DDB', 'TABLE_NAME')
BUCKET = config.get('S3', 'RESULTS_BUCKET')

# Add utility code here
s3 = boto3.client('s3', region_name=REGION_NAME)
glacier = boto3.client('glacier', region_name=REGION_NAME)
dynamodb = boto3.resource('dynamodb', region_name=REGION_NAME)
sqs = boto3.client('sqs', region_name=REGION_NAME)

def move_to_glacier(bucket, key):
    """
    Move an S3 object to Glacier
    """
    try:
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier/client/upload_archive.html
        response = glacier.upload_archive(
            vaultName=GLACIER_VAULT,
            archiveDescription='Archiving ' + key,
            body=s3.get_object(Bucket=bucket, Key=key)['Body'].read()
        )
        archive_id = response['archiveId']
        print(f"Moved {key} to Glacier with archive ID {archive_id}")
        return archive_id
    except Exception as e:
        print(f"Error archiving file to Glacier: {e}")
        return None

def update_dynamoDB(job_id, archive_id):
    """
    Update DynamoDB with the archive ID
    """
    try:
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/table/update_item.html
        table = dynamodb.Table(TABLE_NAME)
        table.update_item(
            Key={'job_id': job_id},
            UpdateExpression='SET results_file_archive_id = :archive_id',
            ExpressionAttributeValues={':archive_id': archive_id}
        )
        print(f"Updated DynamoDB with archive ID {archive_id}")
    except Exception as e:
        print(f"Failed to update DynamoDB: {e}")

def process_file(message):
    try:
        job_id = message['job_id']
        key = message['s3_key_result_file']
        
        archive_id = move_to_glacier(BUCKET, key)
        if archive_id:
            update_dynamoDB(job_id, archive_id)
        else:
            print(f"Failed to archive file, not updating DynamoDB")

        # delete the file from S3
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/delete_object.html
        response = s3.delete_object(Bucket=BUCKET, Key=key)
        print(f"Archived file {key.split('~')[-1]} to glacier for {message['user_id']} successfully")
    except Exception as e:
        print(f"Failed to process message: {e}")

def polling_message():
    while True:
        try:
            # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs/client/receive_message.html
            response = sqs.receive_message(
                QueueUrl=SQS_URL,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=5
            )
            messages = response.get('Messages', [])
            if not messages:
                continue # No messages to process, wait for the next poll

            message = messages[0] 
            body = json.loads(message['Body'])
            data = json.loads(body['Message'])
            user_id = data['user_id']

            # Check if the user is a free user
            user_profile = helpers.get_user_profile(id=user_id)
            if user_profile["role"] == "free_user":
                # query the completion time
                complete_time = datetime.fromtimestamp(data['complete_time'])
                # check if the time has not passed in 5 minutes
                # modify the visibility and handle it later
                if datetime.now() - complete_time < timedelta(minutes=5):
                    print(f"File is not expired yet, waiting for 5 minutes")
                    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs/client/change_message_visibility.html
                    sqs.change_message_visibility(
                        QueueUrl=SQS_URL,
                        ReceiptHandle=message['ReceiptHandle'],
                        VisibilityTimeout=300
                    )
                    continue
                else:
                    # It's safe to archive the expired file for the free user
                    print(f"File is expired, archiving now")
                    process_file(data)  
        except Exception as e:
            print(f"Error in processing message: {e}")
            continue

        try:
            # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs/client/delete_message.html
            sqs.delete_message(
                QueueUrl=SQS_URL,
                ReceiptHandle=message['ReceiptHandle']
            )
        except Exception as e:
            print(f"Error deleting message: {e}")
            break # Break the loop if fail to delete the message

if __name__ == '__main__':
    print("Starting archive process...")
    polling_message()    