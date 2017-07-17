import json
import logging

import requests

import boto3
from botocore.config import Config
from .files import config_file_name, get_s3_json, accounts_file_name, list_prefixes, get_all_accounts, schema_file_name, \
    csv_file_name, get_s3_file, account_file_name, lda_model_name, topic_model_name

log = logging.getLogger(__file__)


def get_pipeline_service(serviceName, job_config):
    pipeline = job_config['Pipeline']
    return next((p for p in pipeline
                 if p['Name'] == serviceName), None)


def get_current_service_name(job_config, default=None):
    pipeline = job_config['Pipeline']
    service = next((p for p in pipeline
                    if p['Status'] == 'pending'), None)
    return service['Name'] if service else default


class S3PipelineService:
    def __init__(self,
                 bucket,
                 service_name,
                 services_config_name='servicesConfig.json'):
        self.bucket = bucket
        self.service_name = service_name
        self.services_config_name = services_config_name
        self._services_config = None
        self.job_configs = {}
        self.accounts = {}
        self.stopwords = {}

    def get_services_config(self, force_refresh=False):
        if not self._services_config or force_refresh:
            self._services_config = get_s3_json(self.bucket, self.services_config_name)
        return self._services_config

    def get_service_config(self, guid):
        job_config = self.get_job_config(guid)
        return get_pipeline_service(self.service_name, job_config)

    def set_service_completed(self, guid):
        service_config = self.get_service_config(guid)
        service_config['Status'] = 'completed'
        self.save_job_config(guid)

    def get_job_pipeline(self, guid):
        return self.get_job_config(guid)['Pipeline']

    def get_next_service(self, guid):
        service_config = self.get_service_config(guid)
        pipeline = self.get_job_pipeline(guid)
        next_service = pipeline[pipeline.index(service_config) + 1]
        next_service_url = self.get_services_config()['ServiceUrls'][next_service['Name']]
        return next_service, next_service_url

    def get_job_config(self, guid):
        if not self.job_configs.get(guid):
            self.job_configs[guid] = get_s3_json(self.bucket, config_file_name(guid))
        return self.job_configs[guid]

    def save_job_config(self, guid):
        job_config = self.get_job_config(guid)
        self.get_bucket().put_object(Key=config_file_name(guid),
                                     Body=json.dumps(job_config,
                                                     ensure_ascii=False))

    def get_accounts(self, guid):
        if not self.accounts.get(guid):
            self.accounts[guid] = get_s3_json(self.bucket, accounts_file_name(guid))
        return self.accounts[guid]

    def iterate_account_files(self, guid, use_cache=True):
        return get_all_accounts(guid, self.get_bucket(), use_cache)

    def get_bucket(self):
        s3 = boto3.resource('s3', config=Config(signature_version='s3v4'))
        return s3.Bucket(self.bucket)

    def get_storage_config(self):
        services_config = self.get_services_config()
        return services_config['Storage']['S3']

    def list_guids(self):
        return list(list_prefixes(self.bucket))

    def enumerate_job_filenames(self, guid):
        return enumerate(self.get_bucket().objects.filter(Prefix='{}/'.format(guid)))

    def save_schema(self, guid, schema):
        self.get_bucket().put_object(Key=schema_file_name(guid),
                                     Body=json.dumps(schema, ensure_ascii=False),
                                     ACL='public-read')

    def save_csv_file(self, guid, file):
        filename = csv_file_name(guid)
        s3 = boto3.resource('s3', config=Config(signature_version='s3v4'))
        s3.meta.client.upload_file(file, self.bucket, filename, ExtraArgs={'ACL': 'public-read'})

    def get_stopwords(self, guid):
        if not self.stopwords.get(guid):
            service_config = self.get_service_config(guid)
            parameters = service_config.get('Parameters', {})
            stopwords = get_s3_file(self.get_bucket(), parameters['StopListFileURL'])
            self.stopwords[guid] = [word.strip() for word in stopwords]
        return self.stopwords[guid]

    def save_account(self, guid, account):
        fname = account_file_name(guid, account['ScreenName'])
        self.get_bucket().put_object(Key=fname, Body=json.dumps(account, ensure_ascii=False), ACL='public-read')

    def call_next_endpoint(self, guid):
        self.set_service_completed(guid)
        next_service, next_service_url = self.get_next_service(guid)
        return requests.get(next_service_url, params={'guid': guid})

    def save_binary_lda_model(self, guid, ldaf):
        model_file_name = lda_model_name(guid)
        self.get_bucket().put_object(Key=model_file_name, Body=ldaf, ACL='public-read')

    def save_topic_model(self, guid, topic_model):
        topic_location = topic_model_name(guid)
        self.get_bucket().put_object(Key=topic_location, Body=json.dumps(topic_model, ensure_ascii=False),
                                     ACL='public-read')
