from multiprocessing import Process
from wsgiref.simple_server import make_server

import boto3
from botocore.config import Config
from hncddpylibs.files import get_s3_json, config_file_name, accounts_file_name, list_prefixes
from hncddpylibs.pipeline import get_pipeline_service, S3PipelineService
from pyramid.config import Configurator

import logging

log = logging.getLogger(__file__)


def setup_web_server(processor, port, bucket, service_name):
    def index(request):
        log.info('NEWJOB_RECEIVED: %s' % request.params)
        guid = request.params['guid']
        p = Process(target=processor, args=(guid, request.job_configurator))
        p.start()
        return {'status': 'started', 'guid': guid}

    config = Configurator()

    def get_job_configurator(request):
        return S3PipelineService(bucket, service_name)

    config.add_request_method(get_job_configurator, 'job_configurator', reify=True)


    config.add_route('index', '/')
    config.add_view(index, route_name='index', renderer='json')
    app = config.make_wsgi_app()
    server = make_server('0.0.0.0', port, app)
    server.serve_forever()

    return config
