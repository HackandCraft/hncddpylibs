import json
import logging

import boto3
import os
import requests
from botocore.config import Config
from io import BytesIO
from pyramid.config import Configurator

log = logging.getLogger(__file__)


def config_file_name(guid):
    return '{0}/{0}-job-config.json'.format(guid)


def accounts_file_name(guid, pattern=None):
    if pattern:
        return pattern.format(guid=guid)
    return '{0}/{0}-twitteraccounts.json'.format(guid)


def account_file_name(guid, screenname):
    return '{}/twitteraccounts/{}.json'.format(guid, screenname)


def account_tmp_file(guid, fname):
    return os.path.join(get_tmp_path(guid), fname.split('/')[-1])


def schema_file_name(guid):
    return '{0}/{0}-schema.json'.format(guid)


def csv_file_name(guid):
    return '{0}/{0}-data.csv'.format(guid)


def lda_model_name(guid):
    return '{0}/{0}-topicmodel.lda'.format(guid)


def topic_model_name(guid):
    return '{0}/{0}-topicmodel.json'.format(guid)


def get_tmp_path(guid=None):
    p = './tmp'
    if not os.path.exists(p):
        os.mkdir(p)
    if guid:
        p = os.path.join(p, guid)
        if not os.path.exists(p):
            os.mkdir(p)
    return p


def get_pipeline_service(serviceName, job_config):
    pipeline = job_config['Pipeline']
    return next((p for p in pipeline
                    if p['Name'] == serviceName), None)


def get_all_accounts(guid, bucketName):
    s3 = boto3.resource('s3', config=Config(signature_version='s3v4'))
    bucket = s3.Bucket(bucketName)
    fullPrefix = '{}/twitteraccounts/'.format(guid)
    for i, obj in enumerate(bucket.objects.filter(Prefix='{}/'.format(guid))):
        if obj.key.startswith(fullPrefix) and obj.key.endswith('json'):
            cache_path = account_tmp_file(guid, obj.key)
            if not os.path.exists(cache_path):
                with open(cache_path, 'wb') as cache:
                    cache.write(obj.get()['Body'].read())
            if os.path.exists(cache_path):
                try:
                    yield json.load(open(cache_path), encoding='utf-8')
                except (json.decoder.JSONDecodeError, UnicodeDecodeError) as e:
                    continue
            else:
                continue


def get_s3_json(bucket, filename):
    s3 = boto3.resource('s3', config=Config(signature_version='s3v4'))
    with BytesIO() as confile:
        s3.Bucket(bucket).download_fileobj(filename, confile)
        confile.seek(0)
        conftext = confile.read().decode('utf-8')
        return json.loads(conftext, encoding='utf-8')


def enrich_config(bucket, SERVICE_NAME):
    config = Configurator()
    techConfig = get_s3_json(bucket, 'servicesConfig.json')

    def get_config(request):
        return techConfig

    def get_job_config(request):
        guid = request.params['guid']
        return get_s3_json(bucket, config_file_name(guid))

    def get_accounts(request):
        guid = request.params['guid']
        service = get_pipeline_service(SERVICE_NAME, request.job_config)
        pattern = service.get('FilePattern')
        return get_s3_json(bucket, accounts_file_name(guid, pattern))

    config.add_request_method(get_config, 'config', property=True)
    config.add_request_method(get_accounts, 'job_accounts', reify=True)
    config.add_request_method(get_job_config, 'job_config', reify=True)
    return config


def process_completor(guid,
                      config,
                      job_config,
                      SERVICE_NAME,
                      S3_BUCKET):
    def ping_next_service(result=None):
        log.info('FINISHED_ASYNC_RESULT: %s', result)
        S3 = boto3.resource('s3', config=Config(signature_version='s3v4'))
        pipeline = job_config['Pipeline']
        service = get_pipeline_service(SERVICE_NAME, job_config)
        service['Status'] = 'completed'
        S3.Bucket(S3_BUCKET).put_object(Key=config_file_name(guid), Body=json.dumps(job_config, ensure_ascii=False))
        try:
            next_service = pipeline[pipeline.index(service) + 1]
            next_service_url = config['ServiceUrls'][next_service['Name']]
        except Exception:
            raise
        else:
            resp = requests.get(next_service_url, params={'guid': guid})
            log.info('NEXT_ENDPOINT_RESPONSE(%s): %s returned %s(%s)',
                     guid, next_service_url, resp.status_code, resp.content)

    return ping_next_service
