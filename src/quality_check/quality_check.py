from __future__ import print_function

import urllib
import os
from BeautifulSoup import BeautifulSoup

def lambda_handler(event, context):
    # Get the file object and bucket names from the event
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.unquote_plus(
        event['Records'][0]['s3']['object']['key']).decode('utf8')

    session_id = context.aws_request_id
    local_path = '/tmp/{}'.format(session_id)

    if not os.path.exists(local_path):
        os.makedirs(local_path)

    filename = os.path.basename(key)
    local_file = os.path.join(local_path, filename)

    try:
        S3.download_file(bucket, key, local_file)

    except Exception as err:
        # This should only fail more than once if permissions are incorrect
        print('Error: Failed to get object {} from bucket {}.'.format(
            key, bucket))
        raise err 


def generateQCConfig(rinex):
    config = BeautifulSoup(open('anubis_base.cfg'))


def parseQCResult(filename):
    return
