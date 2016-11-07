from __future__ import print_function

import urllib
import os
import datetime
import boto3
import botocore
import zlib
import gzip

# Non standard libraries
from lib.BeautifulSoup import BeautifulSoup
from lib.rinex_data import *
from lib.executable import Executable
from lib.elasticsearch import Elasticsearch, RequestsHttpConnection
from lib.aws_requests_auth.aws_auth import AWSRequestsAuth

S3 = boto3.client('s3')

cred = boto3.session.Session().get_credentials()
es_host = 'search-test-qc-nnfncq57wg3kmkpwuaj3t2nkoa.ap-southeast-2.es.amazonaws.com'
auth = AWSRequestsAuth(
    aws_access_key=cred.access_key,
    aws_secret_access_key=cred.secret_key,
    aws_host=es_host,
    aws_region='ap-southeast-2',
    aws_service='es')

es_client = Elasticsearch(
    host=es_host,
    port=80,
    connection_class=RequestsHttpConnection,
    http_auth=auth)

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

    try:
        response = S3.get_object(Bucket=bucket, Key=key)

    except Exception as err:
        # This should only fail more than once if permissions are incorrect
        print('Error: Failed to get object {} from bucket {}.'.format(
            key, bucket))
        raise err

    filename, extension = os.path.splitext(os.path.basename(key))
    local_file = os.path.join(local_path, filename)

    file_data = zlib.decompress(response['Body'].read(), 15+32)
    with open(local_file, 'wb') as out_file:
        out_file.write(file_data)

    rinex_obs = RINEXData(local_file)

    # Get Nav File
    if rinex_obs.marker_name == 'BRDC':
        # QC ALL FILES FOR THAT DAY - HOW TO TRIGGER THIS FOR EACH OF THOSE FILES INDIVIDUALLY?
        print('BRDC file')
        return

    else:
        nav_file = getBRDCNavFile(bucket, rinex_obs.start_time, local_path)
        if nav_file == None:
            print('Daily BRDC file does not yet exist for {}'.format(
                date.strftime('%Y/%j')))
            return

    if rinex_obs.compressed == True:
        rinex_file = hatanaka_decompress(out_file)

    anubis_config, result_file = generateQCConfig(
        rinex_obs, nav_file, local_path)

    anubis = Executable('lib/executables/anubis-2.0.0')
    result = anubis.run('-x {}'.format(anubis_config))

    qc_data = parseQCResult(result_file)


def hatanaka_decompress(local_file):
    CRX2RNX = Executable('lib/executables/CRX2RNX')
    rinex_data = CRX2RNX.run('{} -'.format(local_file))

    if CRX2RNX.returncode > 0:
        raise Exception('CRX2RNX failed with error code {}: {}'.format(
            CRX2RNX.returncode, CRX2RNX.stderr))

    new_name = local_file.replace('.crx', '.rnx')
    
    if new_name == local_file:
        new_name = local_file[:-1] + 'o'

    with open(new_name, 'w') as out_file:
        out_file.write(rinex_data)

    return new_name


def getBRDCNavFile(bucket, date, out_dir):
    """Attempts to get the daily BRDC Nav file for a given date

    Broadcast Navigation file always has prefix public/daily/nav/<year>/<day>/
    Filename is always 'brdc .. RINEX 2 or 3 formatting ..'
    """
    # CHANGE TO DECOMPRESS THE BRDC FILE - NOT USING COMPRESSED DATA WHILE TESTING
    # Also need to sort out RINEX 3 vs RINEX 2 naming issues
    # ALSO CHANGE TO GET SPECIFIC BRDC FOR NON MIXED FILES - or only store mixed Nav files? (RINEX 2?)
    year, day = date.strftime('%Y-%j').split('-')
    brdc = 'public/daily/nav/{}/{}/brdc{}0.{}n.gz'.format(year, day, day, year[2:])

    # Might need to change this to download_file instead of get_object
    # Makes decompression easier - maybe - can probably just decompress byte stream because gzip ....
    try:
        response = S3.get_object(Bucket=bucket, Key=brdc)

    except botocore.exceptions.ClientError as err:
        if err.response['Error']['Code'] == 404:
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

    rinex_obs   RINEXData object
    nav_file    Path to corresponding Navigation file
    output_dir  Directory for quality output file

    Outputs config in output_dir

    Explain Anubis and QC....
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

    # work-around for issue with <beg> and <end> elements needing
    # their contents on the same line as the tags
    # Should be fixed in next version of Anubis
    config_string = base.prettify('utf-8')
    config_string = config_string.replace('<beg>\n', '<beg>')
    config_string = config_string.replace('\n  </beg>', '</beg>')
    config_string = config_string.replace('<end>\n', '<end>')
    config_string = config_string.replace('\n  </end>', '</end>')

    config_file = os.path.join(output_dir, 'config.xml')
    with open(config_file, 'w') as out:
        out.write(config_string)
    
    return config_file, results_file


def parseQCResult(filename):
    """Extract relevant QC metrics from Anubis output file and store in 
    ElasticSearch
    """
    results = BeautifulSoup(open(filename))

    # Map attribute names from Anubis output to Elasticsearch index
    attribute_map = {
        'expZ': 'expected_obs',
        'havZ': 'have_obs',
        'expU': 'expected_obs_10_degrees',
        'havU': 'have_obs_10_degrees',
        'nsat': 'number_sat',
        'mpth': 'multipath',
        'slps': 'cycle_slips'
    }

    docs = ''
    for system in results.qc_gnss.data.findAll('sys'):
        for obs in system.findAll('obs'):
            doc = {
                'site_id': results.qc_gnss.head.site_id.contents[0],
                'system': system['type'],
                'timestamp': datetime.datetime.strptime(
                    results.qc_gnss.data.data_beg.contents[0], '%Y-%m-%d %H:%M:%S'),
                'file_type': 'daily'
            }
            for attribute, value in obs.attrs:
                if attribute == 'type':
                    try:
                        _type, doc['band'], doc['attribute'] = value

                    except ValueError:
                        _type, doc['band'] = value
                        if _type == 'P':
                            _type = 'C'

                        doc['attribute'] = None

                else:
                    doc[attribute_map[attribute]] = value

            if _type not in ['L', 'C']:
                # Only want to store Pseudoranges and Codes
                continue

            types = {'L': 'phase', 'C': 'code'}

            create = {'create': {'_index': 'quality_check', '_type': types[_type]}}
            docs += '{}\n{}\n'.format(create, doc)

    print(docs)
    #es_client.create(
    #    index='quality_metrics',
    #    doc_type='daily',
    #    body=data)

    return
