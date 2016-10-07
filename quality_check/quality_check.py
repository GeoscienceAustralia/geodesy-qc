from __future__ import print_function

import urllib
import os
import datetime
import boto3
import botocore

from BeautifulSoup import BeautifulSoup
from lib.rinex_data import *
from lib.executable import Executable

S3 = boto3.client('s3')

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

    # Get Nav File
    rinex = RINEXData(local_file)

    if rinex.marker_name == 'BRDC':
        # Get list of files which haven't been QC'd
        pass

    else:
        nav = getBRDCNav(bucket, rinex.start_time)

    generateQCConfig(rinex)


def getBRDCNavFile(bucket, date):
    # Floor date
    date = datetime.datetime.strptime(date.strftime('%Y%j'), '%Y%j')
    brdc = 'public/daily/{}/brdc{}0.16d.gz'.format(
        date.strftime('%Y/%j'), date.strftime('%j'))

    try:
        response = S3.get_object(Bucket=bucket, Key=brdc)

    except botocore.exceptions.ClientError as err:
        if err.response['Error']['Code'] == 404:
            print('Daily BRDC file does not yet exist for {}'.format(
                date.strftime('%Y/%j'))
            return

    return response['Body'].read()


def generateQCConfig(rinex):
    base = BeautifulSoup(open('anubis_base.cfg'))

    base.config.gen.beg.contents[0].replaceWith('"{}"'.format(
        rinex.start_time.strftime('%Y-%m-%d 00:00:00')))

    base.config.gen.end.contents[0].replaceWith('"{}"'.format(
        rinex.start_time.strftime('%Y-%m-%d 23:59:59')))

    base.config.gen.sys.contents[0].replaceWith('"GPS"')
    base.config.gen.int.contents[0].replaceWith('1')
    base.config.gen.rec.contents[0].replaceWith(rinex.marker_name)

    base.config.inputs.rinexo.contents[0].replaceWith('"{}"'.format(
        rinex.local_file))
    
    return base


def parseQCResult(filename):
    return
