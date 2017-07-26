from cmreslogging.handlers import CMRESHandler
import socket

KEY_JOB = 'JobId'
KEY_EVENT = 'Event'
EVENT_HANDOVER = 'HandedOverJob'
EVENT_COMPLETED = 'CompletedJob'
EVENT_RECEIVED = 'ReceivedJob'


def evented(guid, event):
    return {'fields': {KEY_JOB: guid, KEY_EVENT: event}}


def unevented(guid, **kwargs):
    result = {'fields': kwargs}
    result['fields'][KEY_JOB] = guid
    return result


class CustomCMRESHandler(CMRESHandler):
    def emit(self, record):
        """ Emit overrides the abstract logging.Handler logRecord emit method

        Format and records the log

        :param record: A class of type ```logging.LogRecord```
        :return: None
        """
        self.format(record)

        rec = self.es_additional_fields.copy()
        for key, value in record.__dict__.items():
            if key not in getattr(CMRESHandler, '_CMRESHandler__LOGGING_FILTER_FIELDS'):
                if rec.get(key) and isinstance(rec.get(key), dict) and isinstance(value, dict):
                    rec[key] = rec[key].copy()
                    rec[key].update(value)
                else:
                    rec[key] = "" if value is None else value
        rec[self.default_timestamp_field_name] = getattr(CMRESHandler, '_CMRESHandler__get_es_datetime_str')(
            record.created)
        rec['message'] = rec.pop('msg')
        with self._buffer_lock:
            self._buffer.append(rec)

        if len(self._buffer) >= self.buffer_size:
            self.flush()
        else:
            self._CMRESHandler__schedule_flush()


def get_es_handler(service_name):
    handler = CustomCMRESHandler(
        hosts=[{'host': 'search-hackandcraft-gvmialq4utx3bxxmvp4ujsmrde.eu-central-1.es.amazonaws.com',
                'port': 80}],
        auth_type=CustomCMRESHandler.AuthType.NO_AUTH,
        es_index_name="datadisruptal-logs",
        es_additional_fields={'fields': {'Service': service_name, 'MachineName': socket.gethostname()}},
        default_timestamp_field_name='@timestamp')
    return handler
