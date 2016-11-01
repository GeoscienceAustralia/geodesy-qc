import boto3
import datetime
import random
from elasticsearch import Elasticsearch, RequestsHttpConnection
from aws_requests_auth.aws_auth import AWSRequestsAuth

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

def generateRecords(start_date, end_date):
    for days in range(int((end_date - start_date).days)):
        date = start_date + datetime.timedelta(days)
        
        for site in ['TEST1', 'TEST2', 'TEST3']:
            for system in ['GPS', 'GLO']:
                for band in [1, 2]:
                    base = {
                        'site_id': site,
                        'system': system,
                        'timestamp': date,
                        'band': band,
                    }
                    code = dict(base)
                    code['type'] = 'C'
                    code['multipath'] = round(random.uniform(0, 2), 2)

                    phase = dict(base)
                    phase['type'] = 'L'
                    phase['cycleSlips'] = random.randrange(0, 20)
        
                    for data in [code, phase]:
                        data['expectedObs'] = random.randrange(25000, 32000)
                        data['haveObs'] = random.randrange(21000, data['expectedObs'])
                        data['expectedObs10Degrees'] = random.randrange(22000, data['expectedObs'])
                        data['haveObs10Degrees'] = random.randrange(20000, data['haveObs'])
                        data['numberSat'] = random.randrange(15, 32)

                        es_client.create(
                            index='quality_metrics',
                            doc_type='daily',
                            body=data)
