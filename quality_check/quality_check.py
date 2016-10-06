from __future__ import print_function

import urllib
import os
import datetime

from BeautifulSoup import BeautifulSoup
from lib.rinex_data import *
from lib.executable import Executable

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

    rinex = RINEXData(local_file)
    
    generateQCConfig(rinex)


def generateQCConfig(rinex):
    base = BeautifulSoup(open('anubis_base.cfg'))

    base.config.gen.beg.contents[0].replaceWith('"{}"'.format(
        rinex.start_time.strftime('%Y-%m-%d %H:%M:%S')))

    end_time = rinex.start_time + datetime.timedelta(
        hours=23, minutes=59, seconds=59)
    base.config.gen.end.contents[0].replaceWith('"{}"'.format(
        end_time.strftime('%Y-%m-%d %H:%M:%S')))

    base.config.gen.sys.contents[0].replaceWith('"GPS"')
    base.config.gen.int.contents[0].replaceWith('1')
    base.config.gen.rec.contents[0].replaceWith(rinex.marker_name)

    base.config.inputs.rinexo.contents[0].replaceWith('"{}"'.format(
        rinex.local_file))
    
    return base


def parseQCResult(filename):
    return
