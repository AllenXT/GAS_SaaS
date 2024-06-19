import sys
import time
from datetime import datetime
import driver
import boto3
import os
import configparser
import json

# Load configuration from ann_config.ini
config = configparser.ConfigParser()
config.read('ann_config.ini')

AWS_REGION = config.get('AWS', 'REGION')
RESULTS_BUCKET = config.get('S3', 'RESULTS_BUCKET')
IDENTIFIER = config.get('IDENTIFIER', 'CNET_ID')
DDB_TABLE_NAME = config.get('DDB', 'TABLE_NAME')
SNS_TOPIC_ARN = config.get('SNS', 'RESULTS_TOPIC_ARN')

# set up the AWS resource
s3_client = boto3.client('s3', region_name=AWS_REGION)
dynamoDB = boto3.resource('dynamodb', region_name=AWS_REGION)
table = dynamoDB.Table(DDB_TABLE_NAME)
sns_client = boto3.client('sns', region_name=AWS_REGION)

"""A rudimentary timer for coarse-grained profiling
"""
class Timer(object):
    def __init__(self, verbose=True):
        self.verbose = verbose

    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *args):
        self.end = time.time()
        self.secs = self.end - self.start
        if self.verbose:
            print(f"Approximate runtime: {self.secs:.2f} seconds")

def upload_to_s3(source_file, bucket, user_id):
    """
    Upload a result and log file to S3
    """
    object_name = os.path.basename(source_file)
    full_object_name = f"{IDENTIFIER}/{user_id}/{object_name}"
    try:
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/upload_file.html
        s3_client.upload_file(source_file, bucket, full_object_name)
        print(f"Uploaded {source_file} to s3://{bucket}/{full_object_name}")
        return full_object_name
    except Exception as e:
        print(f"Failed to upload {source_file}. Error: {str(e)}")
        return None

def update_dynamodb(job_id, result_key, log_key, completion_time):
    """
    Update the DynamoDB table with the completion details
    """
    try:
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/table/update_item.html
        response = table.update_item(
           Key={'job_id': job_id},
           UpdateExpression='SET s3_results_bucket = :bucket, s3_key_result_file = :res, s3_key_log_file = :log, complete_time = :ct, job_status = :status',
           ExpressionAttributeValues={
               ':bucket': RESULTS_BUCKET,
               ':res': result_key,
               ':log': log_key,
               ':ct': completion_time,
               ':status': 'COMPLETED'
            }
        )
        print("DynamoDB update successful")
    except Exception as e:
        print(f"Failed to update DynamoDB: {str(e)}")

def cleanup_local_file(file_path):
    """
    Remove a local working file
    """
    try:
        os.remove(file_path)
        print(f"Deleted local file {file_path}")
    except Exception as e:
        print(f"Failed to delete {file_path}. Error: {str(e)}")

def cleanup_local_directory(directory_path):
    """
    Remove a local working directory
    """
    try:
        os.rmdir(directory_path)
        print(f"Deleted empty working directory")
    except Exception as e:
        print(f"Failed to delete {directory_path}. Error: {str(e)}")

def publish_notification(job_id, result_key, user_id, completion_time, user_email, user_name):
    try:
        msg = {
            'job_id': job_id,
            's3_key_result_file': result_key,
            'user_id': user_id,
            'complete_time': completion_time,
            'user_email': user_email,
            'user_name': user_name,
        }
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sns/client/publish.html
        sns_client.publish(
            TopicArn=SNS_TOPIC_ARN,
            Message=json.dumps({'default': json.dumps(msg)}),
            MessageStructure='json',
        )
        print(f"Job completion notification published")
    except Exception as e:
        print(f"Failed to publish notification: {str(e)}")

if __name__ == '__main__':
    # Call the AnnTools pipeline
    # Expecting at least five arguments
    if len(sys.argv) > 5:
        input_file = sys.argv[1]
        job_id = sys.argv[2]
        user_id = sys.argv[3]
        user_name = sys.argv[4]
        user_email = sys.argv[5]
        with Timer():
            result_file = input_file.replace('.vcf', '.annot.vcf')
            log_file = input_file + '.count.log'
        
            driver.run(input_file, 'vcf')
        
            # Upload the results and log files to S3
            result_key = upload_to_s3(result_file, RESULTS_BUCKET, user_id)
            log_key = upload_to_s3(log_file, RESULTS_BUCKET, user_id)

            # Update DynamoDB with the completion details
            completion_time = int(datetime.now().timestamp())
            update_dynamodb(job_id, result_key, log_key, completion_time)

            # Publish a notification to SNS
            publish_notification(job_id, result_key, user_id, completion_time, user_email, user_name)
        
            # Clean up local files
            cleanup_local_file(result_file)
            cleanup_local_file(log_file)
            cleanup_local_file(input_file)

            # Clean up the left empty working directory
            cleanup_local_directory(os.path.dirname(input_file))
    else:
        print("A valid .vcf file and job_id must be provided as input to this program.")