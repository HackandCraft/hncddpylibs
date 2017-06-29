import boto3
from botocore.config import Config
from hncddpylibs.files import get_s3_json, config_file_name, accounts_file_name, list_prefixes
from hncddpylibs.pipeline import get_pipeline_service
from pyramid.config import Configurator

import logging

log = logging.getLogger(__file__)


def enrich_config(bucket, SERVICE_NAME):
    config = Configurator()
    services_config = get_s3_json(bucket, 'servicesConfig.json')

    def get_config(request):
        return services_config

    def get_job_config(request):
        guid = request.params['guid']
        return get_s3_json(bucket, config_file_name(guid))

    def get_accounts(request):
        guid = request.params['guid']
        service = get_pipeline_service(SERVICE_NAME, request.job_config)
        pattern = service.get('FilePattern')
        return get_s3_json(bucket, accounts_file_name(guid, pattern))

    def get_bucket(request):
        s3 = boto3.resource('s3', config=Config(signature_version='s3v4'))
        return s3.Bucket(bucket)

    def get_storage_config(request):
        return services_config['Storage']['S3']

    def list_guids(request):
        return list(list_prefixes(bucket))

    config.add_request_method(get_config, 'config', property=True)
    config.add_request_method(get_config, 'services_config', property=True)
    config.add_request_method(get_storage_config, 'storage_config', property=True)
    config.add_request_method(get_accounts, 'job_accounts', reify=True)
    config.add_request_method(get_job_config, 'job_config', reify=True)
    config.add_request_method(get_bucket, 'get_bucket', reify=True)
    config.add_request_method(list_guids, 'list_guids')

    return config
