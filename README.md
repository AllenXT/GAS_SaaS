# GAS-Framework

An enhanced web framework (based on [Flask](http://flask.pocoo.org/)) for use in the capstone project. Adds robust user authentication (via [Globus Auth](https://docs.globus.org/api/auth)), modular templates, and some simple styling based on [Bootstrap](http://getbootstrap.com/).

Directory contents are as follows:

* `/web` - The GAS web app files
* `/ann` - Annotator files
* `/util` - Utility scripts for notifications, archival, and restoration
* `/aws` - AWS user data files

## Key Functions

The GAS provides the following functionalities:

* **User Authentication**: Log in via Globus Auth. Supports Free and Premium users.
* **User Upgrade**: Allows Free users to upgrade to Premium.
* **Submit Annotation Jobs**: Free users have limitations on job size; Premium users do not.
* **Job Notifications**: Users receive email notifications when annotation jobs complete.
* **Job Management**: Free users can view and download their annotation job results within a specified time frame, while Premium users have no time restrictions.
* **Data Archival**: Free users' data is archived to Glacier after a specific period.

![Framework Architecture](https://github.com/AllenXT/GAS_SaaS/blob/main/images/framework.png)

## Archive Process

The archive process is designed to manage the data of free users by moving their results files from an S3 bucket to an Amazon Glacier vault after a specific time period.

1. **Completion of Annotation Job** - When a free user's annotation job is completed, a message is sent to the job_result SQS queue. This message contains details about the job, including the user ID, the S3 key of the results file, and the completion timestamp.
2. **Polling the SQS Queue** - It continuously polls the SQS queue to check for new messages. This ensures that the system regularly monitors for completed jobs that need to be archived.
3. **Message Processing** - Upon receiving a message, the script extracts the job details from the message body, then verifies if the user is a free user by checking their profile.
4. **Time Check** - The script checks if the completion time of the job has exceeded the 5-minute window. If it hasn’t, the script resets the message visibility in the queue, delaying its processing until the next poll.
5. **Archiving to Glacier** - If the 5-minute window has passed, the script initiates the archiving process. It retrieves the results file from the S3 bucket and uploads it to the S3 Glacier vault.
6. **Updating DynamoDB** - After successfully archiving the file to Glacier, the script updates a DynamoDB table with the Glacier archive ID. This update links the job record with the archived file, facilitating future retrieval if the user upgrades to a premium plan.
7. **Deleting the S3 Object** - Once the file is archived and the database is updated, the script deletes the original results file from the S3 bucket to free up storage space.
8. **Message Deletion** - Finally, the script deletes the processed message from the SQS queue, ensuring that the same job is not processed multiple times.

## Restore Process

When a free user upgrades to a premium user, the system must restore all of the user’s results files from the Glacier vault back to the S3 bucket.

### Step 1: Initiating the Restore Job (restore.py)

1. **Upgrade Detection** - When a user upgrades from free to premium, a message is sent to an SQS queue indicating this event.
2. **Polling the SQS Queue** - The script continuously polls the SQS queue for new messages indicating that a user has upgraded to premium.
3. **Retrieve Archived Files Information** - Upon detecting a user upgrade, the script queries DynamoDB to retrieve information about all the files for that user, specifically focusing on files that have been archived in Glacier.
4. **Initiate Restoration** - For each archived file, the script attempts to initiate an expedited retrieval from Glacier. If expedited retrieval fails due to insufficient capacity, it falls back to standard retrieval.
5. **Send Notification** - Once the restore job is initiated, a message is sent to an SNS topic to notify that the file restoration process has begun, which helps in decoupling the restoration initiation from the actual file moving process.

### Step 2: Completing the Restore and Moving Files (thaw.py)

1. **Polling the SQS Queue** - The script continuously polls the SQS Queue for messages about initiated restoration jobs.
2. **Check Restore Job Status** - For each restoration job, the script checks the status to determine if the job is still in progress, succeeded, or failed.
3. **Change Message Visibility** - If the job is in progress, the script resets the message visibility in the queue, delaying its processing until the next poll.
4. **Move Restored File to S3** - If the job has succeeded, the script retrieves the restored file from Glacier and uploads it to the S3 bucket.
5. **Update DynamoDB** - After successfully moving the file to S3, the script updates the corresponding DynamoDB record to remove the archive ID, indicating that the file is no longer archived.
6. **Delete from Glacier** - The script then deletes the restored file from Glacier to ensure that storage costs are minimized.
7. **Delete Message from SQS** - Finally, the script deletes the processed message from the SQS queue to prevent reprocessing.

## Rationale for Using SQS

Using AWS SQS decouples the process of generating result files from archiving them. It provides a scalable and reliable way to manage message queuing, allowing for efficient coordination of tasks across distributed services.

## Additional Details

System Components:

* Object store for files
* Key-value store for job information
* Relational database for user accounts
* Annotation service running AnnTools
* Web application for user interaction
* Message queues and notification topics

Scalability:

* Front end served by multiple servers within a load balancer.
* Annotator service scaled with elastic compute infrastructure based on job queue load.

Security:

* HTTPS throughout the site using self-signed certificates.
* Robust Flask WSGI server replaced by Gunicorn.

User Management:

* Implemented using Globus Auth with session-based profile attributes.
* Access control with decorators for authentication and premium user checks.
