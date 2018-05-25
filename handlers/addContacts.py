"""dgl-addContacts-loadCsv
triggered by S3 - object loaded - a csv file of new Contacts
"""

import logging
import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')


def lambda_handler(event, context):

    # retrieve bucket name and file_key from the S3 event
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    file_key = event['Records'][0]['s3']['object']['key']
    logger.info('Reading {} from {}'.format(file_key, bucket_name))
    # get the .csv file object
    obj = s3.get_object(Bucket=bucket_name, Key=file_key)
    # get lines inside the csv
    lines = obj['Body'].read().split(b'\n')
    for r in lines:
        logger.info(r.decode())
#        email_content = email_content + '\n' + r.decode()
#    logger.info(email_content)
