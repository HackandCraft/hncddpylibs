from hncddpylibs.files import get_s3_json, config_file_name, accounts_file_name
from hncddpylibs.pipeline import get_pipeline_service
from pyramid.config import Configurator

import logging

log = logging.getLogger(__file__)


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
