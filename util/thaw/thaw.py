import os
import sys
import boto3
import json

# Import utility helpers
sys.path.insert(1, os.path.realpath(os.path.pardir))
import helpers

# Get configuration
from configparser import SafeConfigParser
config = SafeConfigParser(os.environ)
config.read('thaw_config.ini')

# Add utility code here
# AWS general configuration
REGION_NAME = config.get('AWS', 'REGION')
SQS_URL = config.get('SQS', 'THAW_URL')
GLACIER_VAULT = config.get('GLACIER', 'VAULT')
RESULT_BUCKET = config.get('S3', 'RESULTS_BUCKET')
TABLE_NAME = config.get('DDB', 'TABLE_NAME')

glacier = boto3.client('glacier', region_name=REGION_NAME)
sqs = boto3.client('sqs', region_name=REGION_NAME)
s3 = boto3.client('s3', region_name=REGION_NAME)
dynamodb = boto3.resource('dynamodb', region_name=REGION_NAME)

def polling_messages():
    # handle the request to restore the archived files back to S3
    while True:
        try:
            # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs/client/receive_message.html
            response = sqs.receive_message(
                QueueUrl=SQS_URL,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=3
            )
        except Exception as e:
            print(f"Failed to receive messages to thaw: {e}")
            continue
        
        messages = response.get('Messages', [])
        if not messages:
            continue
        message = messages[0]
        receipt_handle = message['ReceiptHandle']
        body = json.loads(message['Body'])
        data = json.loads(body['Message'])

        annotation_job_id = data['annotation_job_id']
        restoration_job_id = data['restoration_job_id']
        archive_id = data['archive_id']
        s3_key_result_file = data['s3_key_result_file']

        # check restore init job status
        # The status code can be InProgress, Succeeded, or Failed, 
        # and indicates the status of the job.
        status = check_restore_status(restoration_job_id)
        print(f"restore job status: ", status)
        if status == 'Succeeded':
            move_to_s3(restoration_job_id, s3_key_result_file)
            delete_archive(archive_id)
            delete_message(receipt_handle)
            update_dynamodb(annotation_job_id)
            print(f"Restoration job {restoration_job_id} completed successfully")
        elif status == 'InProgress':
            # change the visibility timeout to 900 seconds
            # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs/client/change_message_visibility.html
            sqs.change_message_visibility(
                QueueUrl=SQS_URL,
                ReceiptHandle=receipt_handle,
                VisibilityTimeout=900
            )
            print(f"Restoration job {restoration_job_id} is still in progress")
            continue
        else:
            print(f"Restoration job {restoration_job_id} failed")
            delete_message(receipt_handle)
            continue

def update_dynamodb(annotation_job_id):
    """
    Update the archive ID in DynamoDB
    """
    try:
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/table/update_item.html
        table = dynamodb.Table(TABLE_NAME)
        table.update_item(
            Key={'job_id': annotation_job_id},
            UpdateExpression='SET results_file_archive_id = :archive_id',
            ExpressionAttributeValues={':archive_id': ""}
        )
        print(f"File archive ID updated successfully")
    except Exception as e:
        print(f"Failed to update archive ID for job {annotation_job_id}: {e}")

def delete_message(receipt_handle):
    """
    Delete the message from the queue
    """
    try:
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs/client/delete_message.html
        sqs.delete_message(
            QueueUrl=SQS_URL,
            ReceiptHandle=receipt_handle
        )
        print(f"Restoration message deleted successfully")
    except Exception as e:
        print(f"Failed to delete message: {e}")

def delete_archive(archive_id):
    """
    Delete the archived file from Glacier
    """
    try:
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier/client/delete_archive.html
        glacier.delete_archive(
            vaultName=GLACIER_VAULT,
            archiveId=archive_id
        )
        print(f"Archived file {archive_id} deleted successfully")
    except Exception as e:
        print(f"Failed to delete archived file: {e}")

def move_to_s3(restoration_job_id, s3_key_result_file):
    """
    Move the restored file from Glacier to S3
    """
    try:
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier/client/get_job_output.html
        output = glacier.get_job_output(
            vaultName=GLACIER_VAULT,
            jobId=restoration_job_id
        )
        # read the data stream
        data = output['body'].read()

        # upload the restored file to S3
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/put_object.html
        s3.put_object(
            Bucket=RESULT_BUCKET,
            Key=s3_key_result_file,
            Body=data
        )
        print(f"Restored file {s3_key_result_file} moved to S3 successfully")
    except Exception as e:
        print(f"Failed to move restored file to S3: {e}")
        
def check_restore_status(restoration_job_id):
    """
    Check the status of the restore job
    handle the request to restore the archived files back to S3 if finished
    if not finished, change the visibility timeout to 60 seconds
    """
    try:
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier/client/describe_job.html
        response = glacier.describe_job(
            vaultName=GLACIER_VAULT,
            jobId=restoration_job_id
        )
        return response['StatusCode']
    except Exception as e:
        print(f"Failed to get restore job status: {e}")
        return None

if __name__ == '__main__':
    print("Polling messages to thaw...")
    polling_messages()