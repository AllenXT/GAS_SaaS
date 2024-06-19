# views.py
#
# Copyright (C) 2011-2020 Vas Vasiliadis
# University of Chicago
#
# Application logic for the GAS
#
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import uuid
import time
import json
from datetime import datetime, timedelta

import boto3
from boto3.dynamodb.conditions import Key
from botocore.client import Config
from botocore.exceptions import ClientError

from flask import (abort, flash, redirect, render_template,
  request, session, url_for)

from gas import app, db
from decorators import authenticated, is_premium
from auth import get_profile, update_profile


"""Start annotation request
Create the required AWS S3 policy document and render a form for
uploading an annotation input file using the policy document.

Note: You are welcome to use this code instead of your own
but you can replace the code below with your own if you prefer.
"""
@app.route('/annotate', methods=['GET'])
@authenticated
def annotate():
  # Create a session client to the S3 service
  s3 = boto3.client('s3',
    region_name=app.config['AWS_REGION_NAME'],
    config=Config(signature_version='s3v4'))

  bucket_name = app.config['AWS_S3_INPUTS_BUCKET']
  user_id = session['primary_identity']

  # Generate unique ID to be used as S3 key (name)
  key_name = app.config['AWS_S3_KEY_PREFIX'] + user_id + '/' + \
    str(uuid.uuid4()) + '~${filename}'

  # Create the redirect URL
  redirect_url = str(request.url) + '/job'

  # Define policy fields/conditions
  encryption = app.config['AWS_S3_ENCRYPTION']
  acl = app.config['AWS_S3_ACL']
  fields = {
    "success_action_redirect": redirect_url,
    "x-amz-server-side-encryption": encryption,
    "acl": acl
  }
  conditions = [
    ["starts-with", "$success_action_redirect", redirect_url],
    {"x-amz-server-side-encryption": encryption},
    {"acl": acl}
  ]

  # Generate the presigned POST call
  try:
    presigned_post = s3.generate_presigned_post(
      Bucket=bucket_name, 
      Key=key_name,
      Fields=fields,
      Conditions=conditions,
      ExpiresIn=app.config['AWS_SIGNED_REQUEST_EXPIRATION'])
  except ClientError as e:
    app.logger.error(f"Unable to generate presigned URL for upload: {e}")
    return abort(500)
    
  # Render the upload form which will parse/submit the presigned POST
  return render_template('annotate.html', s3_post=presigned_post)


"""Fires off an annotation job
Accepts the S3 redirect GET request, parses it to extract 
required info, saves a job item to the database, and then
publishes a notification for the annotator service.

Note: Update/replace the code below with your own from previous
homework assignments
"""
@app.route('/annotate/job', methods=['GET'])
@authenticated
def create_annotation_job_request():

  # Get bucket name, key, and job ID from the S3 redirect URL
  bucket_name = str(request.args.get('bucket'))
  s3_key = str(request.args.get('key'))

  # Extract the job ID from the S3 key
  job_id = s3_key.split('/')[2].split('~')[0]
  submit_time = int(datetime.now().timestamp())

  # Persist job to database
  dynamodb = boto3.resource('dynamodb', region_name=app.config['AWS_REGION_NAME'])
  table = dynamodb.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])

  item = {
    'job_id': job_id,
    'user_id': session['primary_identity'],
    'user_name': session['name'],
    'user_email': session['email'],
    'input_file_name': s3_key.split('~')[-1],
    's3_inputs_bucket': bucket_name,
    's3_key_input_file': s3_key,
    'submit_time': submit_time,
    'job_status': 'PENDING'
  }
  try:
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/table/put_item.html
    table.put_item(Item=item)
  except ClientError as e:
    app.logger.error(f"Unable to put item in DynamoDB: {e}")
    return abort(500)

  # Send message to request queue
  sns_client = boto3.client('sns', region_name=app.config['AWS_REGION_NAME'])
  try:
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sns/client/publish.html
    sns_response = sns_client.publish(
      TopicArn=app.config['AWS_SNS_JOB_REQUEST_TOPIC'],
      Message=json.dumps({'default': json.dumps(item)}),
      MessageStructure='json'
    )
  except ClientError as e:
    app.logger.error(f"Unable to publish message to SNS: {e}")
    return abort(500)

  return render_template('annotate_confirm.html', job_id=job_id)


"""List all annotations for the user
"""
@app.route('/annotations', methods=['GET'])
@authenticated
def annotations_list():
  dynamodb = boto3.resource('dynamodb', region_name=app.config['AWS_REGION_NAME'])
  table = dynamodb.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])

  # Get list of annotations to display
  user_id = session['primary_identity']
  # Query DynamoDB using the user ID
  # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/table/query.html
  response = table.query(
    IndexName=app.config['AWS_DYNAMODB_TABLE_INDEX'],
    KeyConditionExpression=Key('user_id').eq(user_id)
  )
  
  annotations = response.get('Items', [])
  # convert timestamps to human-readable format
  for annotation in annotations:
    annotation['submit_time'] = datetime.fromtimestamp(annotation['submit_time']).strftime('%Y-%m-%d %H:%M')
  return render_template('annotations.html', annotations=annotations)

"""download the result file from S3 to the user laptop
Generate a presigned URL to share an S3 object
"""
def generate_presigned_url(bucket_name, object_name, expiration=3600):
  s3 = boto3.client('s3',
    region_name=app.config['AWS_REGION_NAME'],
    config=Config(signature_version='s3v4'))
  try:
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/generate_presigned_url.html
    response = s3.generate_presigned_url('get_object',
      Params={'Bucket': bucket_name, 'Key': object_name},
      ExpiresIn=expiration)
  except ClientError as e:
    app.logger.error(f"Unable to generate presigned URL: {e}")
    return None
  return response

"""Display details of a specific annotation job
"""
@app.route('/annotations/<id>', methods=['GET'])
@authenticated
def annotation_details(id):
  dynamodb = boto3.resource('dynamodb', region_name=app.config['AWS_REGION_NAME'])
  table = dynamodb.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])

  # Get annotation details
  try:
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/table/get_item.html
    response = table.get_item(Key={'job_id': id})
  except ClientError as e:
    app.logger.error(f"Unable to retrieve item from DynamoDB: {e}")
    return abort(500)

  annotation = response.get('Item')
  if not annotation:
    app.logger.error(f"The annotation job {id} does not exist.")
    return abort(404)
  if annotation['user_id'] != session['primary_identity']:
    app.logger.error(f"User {session['primary_identity']} not authorized to view this job")
    return abort(403)
  
  free_access_expired = False
  # create a presigned_url to download the input file
  input_presigned_url = generate_presigned_url(app.config['AWS_S3_INPUTS_BUCKET'], annotation['s3_key_input_file'])
  annotation['input_file_url'] = input_presigned_url
  # convert timestamps to human-readable format
  annotation['submit_time'] = datetime.fromtimestamp(annotation['submit_time']).strftime('%Y-%m-%d %H:%M')
  # Check if the job is completed and expiration time
  if annotation['job_status'] == 'COMPLETED':
    complete_time = datetime.fromtimestamp(annotation['complete_time'])
    annotation['complete_time'] = complete_time.strftime('%Y-%m-%d %H:%M')

    # Determine if the results link is accessible
    if datetime.now() > complete_time + timedelta(minutes=5) and session['role'] == "free_user":
      free_access_expired = True
  
    # Generate a presigned URL to download the result file if not expired for free users
    if not free_access_expired or session['role'] == "premium_user":
      results_presigned_url = generate_presigned_url(app.config['AWS_S3_RESULTS_BUCKET'], annotation['s3_key_result_file'])
      annotation['result_file_url'] = results_presigned_url

  return render_template('annotation_details.html', annotation=annotation, free_access_expired=free_access_expired)


"""Display the log file contents for an annotation job
"""
@app.route('/annotations/<id>/log', methods=['GET'])
@authenticated
def annotation_log(id):
  dynamodb = boto3.resource('dynamodb', region_name=app.config['AWS_REGION_NAME'])
  table = dynamodb.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])

  # Get annotation details
  try:
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/table/get_item.html
    response = table.get_item(Key={'job_id': id})
  except ClientError as e:
    app.logger.error(f"Unable to retrieve item from DynamoDB: {e}")
    return abort(500)

  annotation = response.get('Item')
  if not annotation:
    app.logger.error(f"The annotation job {id} does not exist.")
    return abort(404)
  if annotation['user_id'] != session['primary_identity']:
    app.logger.error(f"User {session['primary_identity']} not authorized to view this job")
    return abort(403)

  # Fetch the log file from S3
  s3 = boto3.client('s3',
    region_name=app.config['AWS_REGION_NAME'],
    config=Config(signature_version='s3v4'))
  try:
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/get_object.html
    log_obj = s3.get_object(Bucket=app.config['AWS_S3_RESULTS_BUCKET'], Key=annotation['s3_key_log_file'])
    # read the log file contents
    log_contents = log_obj['Body'].read().decode('utf-8')
  except ClientError as e:
    app.logger.error(f"Unable to retrieve log file from S3: {e}")
    return abort(500)
  
  # Render log contents to a template
  return render_template('view_log.html', log_file_contents=log_contents, job_id=id)


"""Subscription management handler
"""
@app.route('/subscribe', methods=['GET', 'POST'])
@authenticated
def subscribe():
  if (request.method == 'GET'):
    # Display form to get subscriber credit card info
    if (session.get('role') == "free_user"):
      return render_template('subscribe.html')
    else:
      return redirect(url_for('profile'))

  elif (request.method == 'POST'):
    # Update user role to allow access to paid features
    update_profile(
      identity_id=session['primary_identity'],
      role="premium_user"
    )

    # Update role in the session
    session['role'] = "premium_user"

    # Request restoration of the user's data from Glacier
    # Add code here to initiate restoration of archived user data
    # Make sure you handle files not yet archived!
    message = {
      'user_id': session['primary_identity'],
    }

    sns = boto3.client('sns', region_name=app.config['AWS_REGION_NAME'])
    try:
      # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sns/client/publish.html
      response = sns.publish(
        TopicArn=app.config['AWS_SNS_GLACIER_RESTORE_TOPIC'],
        Message=json.dumps({'default': json.dumps(message)}),
        MessageStructure='json'
      )
      app.logger.info(f"Successfully published message to initiate glacier restoration")
    except ClientError as e:
      app.logger.error(f"Unable to publish message to initiate glacier restoration: {e}")
      return abort(500)

    # Display confirmation page
    return render_template('subscribe_confirm.html') 

"""Reset subscription
"""
@app.route('/unsubscribe', methods=['GET'])
@authenticated
def unsubscribe():
  # Hacky way to reset the user's role to a free user; simplifies testing
  update_profile(
    identity_id=session['primary_identity'],
    role="free_user"
  )
  return redirect(url_for('profile'))


"""DO NOT CHANGE CODE BELOW THIS LINE
*******************************************************************************
"""

"""Home page
"""
@app.route('/', methods=['GET'])
def home():
  return render_template('home.html')

"""Login page; send user to Globus Auth
"""
@app.route('/login', methods=['GET'])
def login():
  app.logger.info(f"Login attempted from IP {request.remote_addr}")
  # If user requested a specific page, save it session for redirect after auth
  if (request.args.get('next')):
    session['next'] = request.args.get('next')
  return redirect(url_for('authcallback'))

"""404 error handler
"""
@app.errorhandler(404)
def page_not_found(e):
  return render_template('error.html', 
    title='Page not found', alert_level='warning',
    message="The page you tried to reach does not exist. \
      Please check the URL and try again."
    ), 404

"""403 error handler
"""
@app.errorhandler(403)
def forbidden(e):
  return render_template('error.html',
    title='Not authorized', alert_level='danger',
    message="You are not authorized to access this page. \
      If you think you deserve to be granted access, please contact the \
      supreme leader of the mutating genome revolutionary party."
    ), 403

"""405 error handler
"""
@app.errorhandler(405)
def not_allowed(e):
  return render_template('error.html',
    title='Not allowed', alert_level='warning',
    message="You attempted an operation that's not allowed; \
      get your act together, hacker!"
    ), 405

"""500 error handler
"""
@app.errorhandler(500)
def internal_error(error):
  return render_template('error.html',
    title='Server error', alert_level='danger',
    message="The server encountered an error and could \
      not process your request."
    ), 500

### EOF
