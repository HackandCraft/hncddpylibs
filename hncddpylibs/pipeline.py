import json
import logging

import requests

import boto3
from botocore.config import Config
from hncddpylibs.files import config_file_name

log = logging.getLogger(__file__)


def get_pipeline_service(serviceName, job_config):
    pipeline = job_config['Pipeline']
    return next((p for p in pipeline
                 if p['Name'] == serviceName), None)


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
