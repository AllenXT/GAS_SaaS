import os
import sys
import boto3
import json
from boto3.dynamodb.conditions import Key

# Import utility helpers
sys.path.insert(1, os.path.realpath(os.path.pardir))
import helpers

# Get configuration
from configparser import SafeConfigParser
config = SafeConfigParser(os.environ)
config.read('restore_config.ini')

# Add utility code here
# AWS general configuration
REGION_NAME = config.get('AWS', 'REGION')
TABLE_NAME = config.get('DDB', 'TABLE_NAME')
SQS_URL = config.get('SQS', 'RESTORE_URL')
GLACIER_VAULT = config.get('GLACIER', 'VAULT')
SNS_TOPIC_ARN = config.get('SNS', 'THAW_TOPIC_ARN')

glacier = boto3.client('glacier', region_name=REGION_NAME)
dynamodb = boto3.resource('dynamodb', region_name=REGION_NAME)
sqs = boto3.client('sqs', region_name=REGION_NAME)
sns = boto3.client('sns', region_name=REGION_NAME)

def polling_messages():
    # poll messages to initiate restoration for new premium users
    while True:
        try:
            # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs/client/receive_message.html
            response = sqs.receive_message(
                QueueUrl=SQS_URL,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=5
            )
        except Exception as e:
            print(f"Failed to receive messages to initiate restoration: {e}")
            continue
        
        messages = response.get('Messages', [])
        if not messages:
            continue
        message = messages[0]
        receipt_handle = message['ReceiptHandle']
        body = json.loads(message['Body'])
        data = json.loads(body['Message'])

        user_id = data['user_id']
        # get all this user's files with archived ids from dynamodb
        try:
            table = dynamodb.Table(TABLE_NAME)
            # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/table/query.html
            user_files = table.query(
                IndexName=config.get('DDB', 'INDEX_NAME'),
                KeyConditionExpression=Key('user_id').eq(user_id)
            )
            print(f"Got files info from DynamoDB for user {user_id}")
        except Exception as e:
            print(f"Failed to get files info from DynamoDB: {e}")
            continue
        
        # iterate over the user's files and initiate restoration
        for file in user_files['Items']:
            job_id = file['job_id']
            # we only restore files that has been archived
            if "results_file_archive_id" in file and file['results_file_archive_id'] != "":
                archive_id = file['results_file_archive_id']
                s3_result_key = file['s3_key_result_file']
                response = init_restore(archive_id)
                if response:
                    print(f"Initiated restoration for archived file {s3_result_key.split('~')[-1]} with job ID {job_id} successfully")
                    message = {
                        'annotation_job_id': job_id,
                        'restoration_job_id': response['jobId'],
                        'archive_id': archive_id,
                        's3_key_result_file': s3_result_key,  
                    }
                    # post a message to the SNS topic to notify that move the object back to S3
                    try:
                        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sns/client/publish.html
                        sns_response = sns.publish(
                            TopicArn=SNS_TOPIC_ARN,
                            Message=json.dumps({'default': json.dumps(message)}),
                            MessageStructure='json'
                        )
                        print(f"Posted message to SNS topic to thaw successfully")
                    except Exception as e:
                        print(f"Failed to post message to SNS topic to thaw: {e}")
                else:
                    print(f"Failed to initiate restoration for archived file {s3_result_key.split('~')[-1]} with job ID {job_id}")

        # delete the message from the queue
        try:
            # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs/client/delete_message.html
            sqs.delete_message(
                QueueUrl=SQS_URL,
                ReceiptHandle=receipt_handle
            )
        except Exception as e:
            print(f"Failed to delete message from the restore queue: {e}")    

def init_restore(archive_id):
    """
    first attempt to use Expedited retrievals from Glacier
    if that fails, use Standard retrievals
    """
    try:
        # Attempt to expedite retrieval
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier/client/initiate_job.html
        response = glacier.initiate_job(
            vaultName=GLACIER_VAULT,
            jobParameters={
                'Type': 'archive-retrieval',
                'ArchiveId': archive_id,
                'Tier': 'Expedited'
            }
        )
        print(f"Initiated expedited retrieval for archive {archive_id}")
        return response
    except glacier.exceptions.InsufficientCapacityException:
        # Fallback to standard retrieval if expedited fails
        print(f"Expedited retrieval failed, falling back to Standard for archive {archive_id}")
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier/client/initiate_job.html
        response = glacier.initiate_job(
            vaultName=GLACIER_VAULT,
            jobParameters={
                'Type': 'archive-retrieval',
                'ArchiveId': archive_id,
                'Tier': 'Standard'
            }
        )
        return response
    except Exception as e:
        print(f"Failed to initiate restoration for archive {archive_id}: {e}")
        return None

if __name__ == '__main__':
    print("Starting restore process...")
    polling_messages()