import logging
from multiprocessing import Process
from wsgiref.simple_server import make_server

from hncddpylibs.pipeline import S3PipelineService
from pyramid.config import Configurator

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
