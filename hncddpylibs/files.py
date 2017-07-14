import json

import re

from botocore.config import Config
from io import BytesIO

import os
import boto3
import logging

uuid4hex = re.compile('[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}', re.I)
log = logging.getLogger(__file__)


def config_file_name(guid):
    return '{0}/{0}-job-config.json'.format(guid)


def evaluations_file_name(guid):
    return '{0}/{0}-evaluations.json'.format(guid)


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


def get_all_accounts(guid, bucket, use_cache=True):
    fullPrefix = '{}/twitteraccounts/'.format(guid)
    for i, obj in enumerate(bucket.objects.filter(Prefix='{}/'.format(guid))):
        if obj.key.startswith(fullPrefix) and obj.key.endswith('json'):
            cache_path = account_tmp_file(guid, obj.key)
            if not os.path.exists(cache_path) or use_cache == False:
                with open(cache_path, 'wb') as cache:
                    cache.write(obj.get()['Body'].read())
            if os.path.exists(cache_path):
                try:
                    yield json.load(open(cache_path, encoding='utf-8'), encoding='utf-8')
                except (json.decoder.JSONDecodeError, UnicodeDecodeError) as e:
                    log.error('%s: %s', obj.key, e)
                    continue
            else:
                continue


def get_s3_file(bucket, filename):
    s3 = boto3.resource('s3', config=Config(signature_version='s3v4'))
    with BytesIO() as confile:
        try:
            s3.Bucket(bucket).download_fileobj(filename, confile)
            confile.seek(0)
            return confile.read().decode('utf-8')
        except Exception as e:
            log.error('ERROR DOWENLOADING JSON:%s: %s', filename, e)
    return None


def get_s3_json(bucket, filename):
    conftext = get_s3_file(bucket, filename)
    return json.loads(conftext, encoding='utf-8')


def list_prefixes(bucket):
    client = boto3.client('s3', config=Config(signature_version='s3v4'))
    paginator = client.get_paginator('list_objects')
    result = paginator.paginate(Bucket=bucket, Delimiter='/')
    prefixes = (prefix.get('Prefix').strip('/') for prefix in result.search('CommonPrefixes'))
    return filter(uuid4hex.match, prefixes)


def get_all_job_configs(bucket):
    guids = list_prefixes(bucket)
    return ((guid, get_s3_json(bucket, config_file_name(guid)))
            for guid in guids)
