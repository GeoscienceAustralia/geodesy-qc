import boto3
import datetime
import json

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

def lambda_handler(event, context):
    """
    """
    lambda_client = boto3.client('lambda')
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.unquote_plus(
        event['Records'][0]['s3']['object']['key']).decode('utf8')

    year, day = key.split('/')[3:5]
    prefix = '/daily/obs/{}/{}/'.format(year, day)

    for s3_obj in getKeys(bucket, ['public' + prefix, 'private' + prefix]):
        request = lambdaTriggerParams
        request['Records'][0]['s3']['object']['key'] = s3_obj
        request['Records'][0]['s3']['bucket']['name'] = bucket

        try:
            lambda_client.invoke_async(
                FunctionName='test-QC',
                InvokeArgs=json.dumps(request))

        except Exception as err:
            print('Invocation of Quality Check failed for {}\n{}'.format(
                key, err))
            pass


def getKeys(bucket, prefixes):
    """
    """
    s3_client = boto3.client('s3')

    if type(prefixes) is not list:
        prefixes = [prefixes]

    keys = []
    for prefix in prefixes:
        paginator = s3_client.get_paginator('list_objects')
        page_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)

        for page in page_iterator:
            if 'Contents' in page:
                keys += [obj['Key'] for obj in page['Contents']]

    return keys
