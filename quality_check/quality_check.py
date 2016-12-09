from __future__ import print_function

import urllib
import os
import shutil
import datetime
import boto3
import botocore
import zlib
import gzip
import sys
import json

# Non standard libraries packaged in ./lib directory
lib = os.path.abspath('lib/')
sys.path.append(lib)

# Non standard libraries
from BeautifulSoup import BeautifulSoup
from rinex_data import *
from executable import Executable
from elasticsearch import Elasticsearch, RequestsHttpConnection
from aws_requests_auth.aws_auth import AWSRequestsAuth

S3 = boto3.client('s3')

def lambda_handler(event, context):
    # Get the file object and bucket names from the event
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.unquote_plus(
        event['Records'][0]['s3']['object']['key']).decode('utf8')

    print('Quality Check: Key {}'.format(key))

    status, file_type, data_type, year, day = key.split('/')[:5]

    if data_type == 'nav':
        nav_file = os.path.basename(key)
        if nav_file[:4].lower() == 'brdc' and nav_file[-4:] == 'n.gz':
            triggerQCFromNav(year, day, context.function_name, bucket)

        else:
            print('Do not Quality Check using non-Broadcast Navigation data')

        return

    # Use AWS request ID from context object for unique directory
    session_id = context.aws_request_id
    local_path = '/tmp/{}'.format(session_id)

    if not os.path.exists(local_path):
        os.makedirs(local_path)

    try:
        response = S3.get_object(Bucket=bucket, Key=key)

    except Exception as err:
        # This should only fail more than once if permissions are incorrect
        print('Error: Failed to get object {} from bucket {}.'.format(
            key, bucket))
        raise err

    # Decompress Observation file and store locally
    filename, extension = os.path.splitext(os.path.basename(key))
    local_file = os.path.join(local_path, filename)

    file_data = zlib.decompress(response['Body'].read(), 15+32)
    with open(local_file, 'wb') as out_file:
        out_file.write(file_data)

    # Parse RINEX file
    rinex_obs = RINEXData(local_file)

    # Attempt to get Broadcast Navigation file from archive
    nav_file = getBRDCNavFile(bucket, rinex_obs.start_time, local_path)
    if nav_file == None:
        print('Daily BRDC file does not yet exist for {}/{}'.format(
            year, day))
        return

    # Hatanaka decompress RINEX file if needed 
    if rinex_obs.compressed == True:
        rinex_obs.local_file = hatanaka_decompress(rinex_obs.local_file)

    # Generate an Anubis XML config file
    anubis_config, result_file = generateQCConfig(
        rinex_obs, nav_file, local_path)

    # Run Anubis with the generated config file as input
    anubis = Executable('lib/executables/anubis-2.0.1')
    anubis_log = anubis.run('-x {}'.format(anubis_config))
    if anubis.returncode > 0:
        print('Anubis errored with return code {}: {}\n{}'.format(
            anubis.returncode, anubis.stderr, anubis.stdout))
        return

    # Parse results of Anubis
    parseQCResult(result_file, key)

    # Delete tmp working space and Anubis copy to resolve Lambda disk 
    # space allocation issue
    shutil.rmtree(local_path)

    return


def hatanaka_decompress(local_file):
    """Hatanaka decompresses a local file using the CRX2RNX program
    Outputs data to new file with correct name under same directory as input

    Input:
        local_file  path to Hatanaka compressed RINEX file

    Returns:
        new_name    name of created decompressed RINEX file
    """
    # Check if CRX2RNX is in /tmp - where Lambda instances are throttled
    if os.path.isfile('/tmp/CRX2RNX'):
        CRX2RNX = Executable('/tmp/CRX2RNX', True)

    else:
        CRX2RNX = Executable('lib/executables/CRX2RNX')

    rinex_data = CRX2RNX.run('{} -'.format(local_file))

    if CRX2RNX.returncode > 0:
        raise Exception('CRX2RNX failed with error code {}: {}'.format(
            CRX2RNX.returncode, CRX2RNX.stderr))

    # RINEX 3 file extension changes from crx to rnx when decompressed
    new_name = local_file.replace('.crx', '.rnx')
    
    # Hatanaka compressed RINEX 2 files are suffixed with d, replace with o
    if new_name == local_file:
        new_name = local_file[:-1] + 'o'

    with open(new_name, 'w') as out_file:
        out_file.write(rinex_data)

    return new_name


def getBRDCNavFile(bucket, date, out_dir):
    """Attempts to get the daily BRDC Nav file for a given date from archive

    Broadcast Navigation file always has prefix public/daily/nav/<year>/<day>/
    Filename is always 'brdc..'
    """
    year, day = date.strftime('%Y-%j').split('-')
    brdc = 'public/daily/nav/{}/{}/brdc{}0.{}n.gz'.format(year, day, day, year[2:])

    try:
        response = S3.get_object(Bucket=bucket, Key=brdc)

    except botocore.exceptions.ClientError as err:
        if err.response['Error']['Code'] == 'NoSuchKey':
            # BRDC File does not yet exist, do nothing
            return

        raise

    out_file = os.path.join(out_dir, os.path.basename(brdc))
    # Decompress Nav data, must be .gz file given the GET request asks for one
    nav_data = zlib.decompress(response['Body'].read(), 15+32)
    
    with open(out_file, 'w') as output:
        output.write(nav_data)

    return out_file


def generateQCConfig(rinex_obs, nav_file, output_dir):
    """Generates Anubis configuration file given the following: 

    Input:
        rinex_obs       RINEXData object
        nav_file        path to corresponding Navigation file
        output_dir      directory for quality output file

    Returns:
        config_file     file containing generated config file
        results_file    file which Anubis will output results to
    """
    base = BeautifulSoup(open('anubis_base.cfg'))

    # Currently assuming that start and end time are start and end of day
    base.config.gen.beg.contents[0].replaceWith('"{}"'.format(
        rinex_obs.start_time.strftime('%Y-%m-%d 00:00:00')))

    base.config.gen.end.contents[0].replaceWith('"{}"'.format(
        rinex_obs.start_time.strftime('%Y-%m-%d 23:59:59')))

    base.config.gen.rec.contents[0].replaceWith(rinex_obs.marker_name)

    base.config.inputs.rinexo.contents[0].replaceWith(rinex_obs.local_file)

    base.config.inputs.rinexn.contents[0].replaceWith(nav_file)

    results_file = os.path.join(output_dir, 'output.xml')
    base.config.outputs.xml.contents[0].replaceWith(results_file)

    config_file = os.path.join(output_dir, 'config.xml')
    with open(config_file, 'w') as out:
        out.write(base.prettify('utf-8'))
    
    return config_file, results_file


def parseQCResult(filename, key):
    """Extract relevant QC metrics from Anubis output file and store in 
    ElasticSearch

    Only takes key for inclusion in ES documents
    """
    # Create authenticated connection to Elasticsearch cluster
    es_host = 'search-gnss-datacenter-es-2a65hbml7jlcthde6few3g7yxi.ap-southeast-2.es.amazonaws.com'
    es_index = 'quality_metrics'

    cred = boto3.session.Session().get_credentials()
    auth = AWSRequestsAuth(
        aws_access_key=cred.access_key,
        aws_secret_access_key=cred.secret_key,
        aws_token=cred.token,
        aws_host=es_host,
        aws_region='ap-southeast-2',
        aws_service='es')

    es_client = Elasticsearch(
        host=es_host,
        port=80,
        connection_class=RequestsHttpConnection,
        http_auth=auth)

    # Read results XML file
    results = BeautifulSoup(open(filename))

    # Map attribute names from Anubis output to Elasticsearch index names and types
    attribute_map = {
        'expz': ('expected_obs', int),
        'havz': ('have_obs', int),
        'expu': ('expected_obs_10_degrees', int),
        'havu': ('have_obs_10_degrees', int),
        'nsat': ('number_sat', int),
        'mpth': ('multipath', float),
        'slps': ('cycle_slips', int)
    }

    # Extract relevant fields from results file
    for system in results.qc_gnss.data.findAll('sys'):
        for obs in system.findAll('obs'):
            doc = {
                'site_id': results.qc_gnss.head.site_id.contents[0],
                'system': system['type'],
                'timestamp': datetime.datetime.strptime(
                    results.qc_gnss.data.data_beg.contents[0], '%Y-%m-%d %H:%M:%S'),
                'file_type': 'daily',
                'filename': key
            }
            for attribute, value in obs.attrs:
                if attribute == 'type':
                    try:
                        _type, band, doc['attribute'] = value

                    except ValueError:
                        _type, band = value
                        doc['attribute'] = None

                    doc['band'] = int(band)

                else:
                    attr_name, attr_type = attribute_map[attribute]
                    doc[attr_name] = attr_type(value)

            if _type not in ['L', 'C']:
                # Only want to store Pseudoranges and Codes
                continue

            # Convert type to full name, L=phase C=code
            types = {'L': 'phase', 'C': 'code'}

            # ID is composite field so that reprocessed files will overwrite old data
            doc_id = '{}{}{}{}{}{}'.format(
                doc['site_id'], doc['system'], doc['timestamp'], 
                doc['file_type'], doc['band'], doc['attribute'])

            # Submit record to Elasticsearch
            es_client.index(
                index=es_index, doc_type=types[_type], body=doc, id=doc_id)

    return


def triggerQCFromNav(year, day, function_name, bucket):
    """Invoke a lambda function with PUT operations for all objects in archive 
    for a given year and day of year
    """
    lambda_client = boto3.client('lambda')
    # Define base trigger parameters
    lambdaTriggerParams = {
        "Records": [
            {
                "eventTime": datetime.datetime.utcnow().isoformat() + 'Z',
                "s3": {
                    "object": {
                        "key": "",
                    },
                    "bucket": {
                        "name": "",
                    },
                    "s3SchemaVersion": "1.0"
                },
                "awsRegion": "ap-southeast-2",
                "eventName": "ObjectCreated:Put",
                "eventSource": "aws:s3"
            }
        ]
    }

    # Loop through all public and private daily obs files for given day
    prefix = '/daily/obs/{}/{}/'.format(year, day)
    for s3_obj in getKeys(bucket, ['public' + prefix, 'private' + prefix]):
        request = lambdaTriggerParams
        request['Records'][0]['s3']['object']['key'] = s3_obj
        request['Records'][0]['s3']['bucket']['name'] = bucket

        try:
            # Attempt to invoke lambda function
            lambda_client.invoke_async(
                FunctionName=function_name,
                InvokeArgs=json.dumps(request))

        except Exception as err:
            print('Invocation of {} Lambda failed for key {}\n{}'.format(
                function_name, s3_obj, err))
            pass


def getKeys(bucket, prefixes):
    """Get a list of S3 objects in a bucket with a given prefix

    Paginates responses in case there are over 1000 entries

    prefixes can be single prefix, or list of prefixes
    """
    if type(prefixes) is not list:
        prefixes = [prefixes]

    keys = []
    for prefix in prefixes:
        paginator = S3.get_paginator('list_objects')
        page_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)

        for page in page_iterator:
            if 'Contents' in page:
                keys += [obj['Key'] for obj in page['Contents']]

    return keys

