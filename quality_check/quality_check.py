from __future__ import print_function

import urllib
import os
import datetime
import boto3
import botocore

# Non standard libraries
from lib.BeautifulSoup import BeautifulSoup
from lib.rinex_data import *
from lib.executable import Executable

S3 = boto3.client('s3')

def lambda_handler(event, context):
    # Get the file object and bucket names from the event
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.unquote_plus(
        event['Records'][0]['s3']['object']['key']).decode('utf8')

    # Use AWS request ID from context object for unique directory
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

    rinex_obs = RINEXData(local_file)

    # Get Nav File
    if rinex_obs.marker_name == 'BRDC':
        # QC ALL FILES FOR THAT DAY - HOW TO TRIGGER THIS FOR EACH OF THOSE FILES INDIVIDUALLY?
        pass

    else:
        nav_file = getBRDCNavFile(bucket, rinex_obs.start_time, local_path)

    anubis_config = generateQCConfig(rinex_obs, nav_file, local_path)
    print(anubis_config)


def getBRDCNavFile(bucket, date, out_dir):
    # CHANGE TO DECOMPRESS THE BRDC FILE - NOT USING COMPRESSED DATA WHILE TESTING
    # Also need to sort out RINEX 3 vs RINEX 2 naming issues
    brdc = 'public/daily/{}/brdc{}0.{}g'.format(
        date.strftime('%Y/%j'), date.strftime('%j'), str(date.year)[2:])

    # Might need to change this to download_file instead of get_object
    # Makes decompression easier - maybe - can probably just decompress byte stream ....
    try:
        response = S3.get_object(Bucket=bucket, Key=brdc)

    except botocore.exceptions.ClientError as err:
        if err.response['Error']['Code'] == 404:
            # BRDC File does not yet exist, do nothing
            print('Daily BRDC file does not yet exist for {}'.format(
                date.strftime('%Y/%j')))
            return

        raise

    out_file = os.path.join(out_dir, os.path.basename(brdc))
    with open(out_file, 'w') as output:
        output.write(response['Body'].read())

    return out_file


def generateQCConfig(rinex_obs, nav_file, output_dir):
    base = BeautifulSoup(open('anubis_base.cfg'))

    base.config.gen.beg.contents[0].replaceWith('"{}"'.format(
        rinex_obs.start_time.strftime('%Y-%m-%d 00:00:00')))

    base.config.gen.end.contents[0].replaceWith('"{}"'.format(
        rinex_obs.start_time.strftime('%Y-%m-%d 23:59:59')))

    base.config.gen.rec.contents[0].replaceWith(rinex_obs.marker_name)

    base.config.inputs.rinexo.contents[0].replaceWith(rinex_obs.local_file)

    base.config.inputs.rinexn.contents[0].replaceWith(nav_file)

    base.config.outputs.xml.contents[0].replaceWith(
        os.path.join(output_dir, 'output.xml'))
    
    return base


def parseQCResult(filename):
    return
