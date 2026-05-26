# Copyright 2026
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import atexit
import base64
from datetime import datetime
from datetime import timezone
import inspect
import json
import os
import queue
import threading
import traceback
import urllib.error
import urllib.request

from oslo_log import log as logging


LOG = logging.getLogger(__name__)


def _utcnow():
    return datetime.now(timezone.utc).isoformat()


def _normalize_token(value):
    if not value:
        return None
    token = ''.join(
        char.lower() if char.isalnum() else '_'
        for char in value.strip())
    token = '_'.join(filter(None, token.split('_')))
    return token[:80] if token else None


def _safe_test_id(test):
    try:
        return test.id()
    except Exception:
        return str(test)


def _extract_details(details):
    if not details:
        return None

    extracted = {}
    for key, value in details.items():
        try:
            extracted[key] = ''.join(value.iter_text())
        except Exception:
            extracted[key] = str(value)
    return extracted


def _extract_error_message(err=None, details=None):
    if err:
        try:
            return ''.join(traceback.format_exception_only(err[0], err[1])).strip()
        except Exception:
            return str(err)

    extracted = _extract_details(details)
    if extracted:
        traceback_text = extracted.get('traceback')
        if traceback_text:
            lines = [line.strip() for line in traceback_text.splitlines()
                     if line.strip()]
            if lines:
                return lines[-1]
        for key in ('traceback', 'reason', 'content', 'stderr'):
            if extracted.get(key):
                return extracted[key]
        return next(iter(extracted.values()))
    return None


def _guess_service(module_name):
    tokens = module_name.split('.')
    if 'compute' in tokens:
        return 'nova'
    if 'network' in tokens:
        return 'neutron'
    if 'volume' in tokens:
        return 'cinder'
    if 'image' in tokens:
        return 'glance'
    if 'identity' in tokens:
        return 'keystone'
    if 'object_storage' in tokens:
        return 'swift'
    if 'orchestration' in tokens:
        return 'heat'
    if 'baremetal' in tokens:
        return 'ironic'
    if 'placement' in tokens:
        return 'placement'
    if 'share' in tokens:
        return 'manila'
    return None


def _guess_layer(module_name):
    tokens = module_name.split('.')
    if 'scenario' in tokens:
        return 'scenario'
    if 'api' in tokens:
        return 'api'
    return None


def _failure_signature(test_id, message, primary_service=None):
    if not message:
        return None
    first_line = message.splitlines()[0].strip()
    normalized = _normalize_token(first_line)
    if not normalized:
        return None
    prefix = primary_service or _guess_service(test_id) or 'tempest'
    return '%s:%s' % (prefix, normalized)


class AsyncEventEmitter:
    """Emit Tempest test events asynchronously.

    Enabled via environment variables:
      * TEMPEST_EVENT_STREAM_ENABLED=1
      * TEMPEST_EVENT_STREAM_FILE=/path/to/events.ndjson (optional)
      * TEMPEST_EVENT_STREAM_URL=https://endpoint.example/events (optional)
      * TEMPEST_EVENT_RUN_ID=custom-run-id (optional)

    If neither FILE nor URL are configured, events go to stdout as NDJSON.
    """

    def __init__(self):
        self.enabled = os.getenv('TEMPEST_EVENT_STREAM_ENABLED', '').lower() in (
            '1', 'true', 'yes', 'on')
        self.file_path = os.getenv('TEMPEST_EVENT_STREAM_FILE')
        self.url = os.getenv('TEMPEST_EVENT_STREAM_URL')
        self.opensearch_url = os.getenv('TEMPEST_EVENT_OPENSEARCH_URL')
        self.opensearch_index = os.getenv(
            'TEMPEST_EVENT_OPENSEARCH_INDEX', 'tempest-test-event')
        self.username = os.getenv('TEMPEST_EVENT_STREAM_USERNAME')
        self.password = os.getenv('TEMPEST_EVENT_STREAM_PASSWORD')
        self.run_id = os.getenv('TEMPEST_EVENT_RUN_ID') or _utcnow()
        self.job_name = os.getenv('TEMPEST_EVENT_JOB_NAME')
        self.branch = os.getenv('TEMPEST_EVENT_BRANCH')
        self.worker = os.getenv('TEMPEST_EVENT_WORKER')
        self.build_url = os.getenv('TEMPEST_EVENT_BUILD_URL')
        self._queue = queue.Queue()
        self._thread = None
        self._shutdown = threading.Event()

        if self.enabled:
            self._thread = threading.Thread(
                target=self._drain, name='tempest-event-stream', daemon=True)
            self._thread.start()
            atexit.register(self.close)

    def emit(self, event):
        if not self.enabled:
            return
        event.setdefault('run_id', self.run_id)
        event.setdefault('timestamp', _utcnow())
        event.setdefault('job_name', self.job_name)
        event.setdefault('branch', self.branch)
        event.setdefault('worker', self.worker)
        event.setdefault('build_url', self.build_url)
        self._queue.put(event)

    def close(self):
        if not self.enabled or self._shutdown.is_set():
            return
        self._shutdown.set()
        self._queue.put(None)
        if self._thread:
            self._thread.join(timeout=2)

    def _drain(self):
        while True:
            event = self._queue.get()
            if event is None:
                return
            try:
                self._write_event(event)
            except Exception:
                LOG.exception('Failed to write Tempest realtime test event')

    def _write_event(self, event):
        payload = json.dumps(event, ensure_ascii=False)

        if self.file_path:
            with open(self.file_path, 'a', encoding='utf-8') as stream:
                stream.write(payload + '\n')

        if self.url:
            request = urllib.request.Request(
                self.url,
                data=payload.encode('utf-8'),
                headers=self._headers('application/json'),
                method='POST')
            try:
                with urllib.request.urlopen(request, timeout=1):
                    pass
            except urllib.error.URLError:
                LOG.exception('Failed to POST Tempest test event to %s', self.url)

        if self.opensearch_url:
            action = json.dumps({
                'index': {
                    '_index': self.opensearch_index,
                    '_id': '%s:%s:%s' % (
                        event.get('run_id'),
                        event.get('test_id'),
                        event.get('event_type'))
                }
            })
            bulk_payload = action + '\n' + payload + '\n'
            request = urllib.request.Request(
                self.opensearch_url.rstrip('/') + '/_bulk',
                data=bulk_payload.encode('utf-8'),
                headers=self._headers('application/x-ndjson'),
                method='POST')
            try:
                with urllib.request.urlopen(request, timeout=1):
                    pass
            except urllib.error.URLError:
                LOG.exception('Failed to POST Tempest test event to OpenSearch %s',
                              self.opensearch_url)

        if not self.file_path and not self.url and not self.opensearch_url:
            print(payload, flush=True)

    def _headers(self, content_type):
        headers = {'Content-Type': content_type}
        if self.username and self.password:
            token = ('%s:%s' % (self.username, self.password)).encode('utf-8')
            headers['Authorization'] = 'Basic ' + (
                base64.b64encode(token).decode('ascii'))
        return headers


_EMITTER = AsyncEventEmitter()


def get_event_emitter():
    return _EMITTER


class EventStreamResultProxy:
    """Proxy a TestResult and emit realtime test events."""

    def __init__(self, result, emitter):
        self._result = result
        self._emitter = emitter
        self._started = {}

    def __getattr__(self, name):
        return getattr(self._result, name)

    def _call_result_method(self, name, test, err=None, reason=None,
                            details=None):
        method = getattr(self._result, name)
        params = inspect.signature(method).parameters
        kwargs = {}
        if 'err' in params and err is not None:
            kwargs['err'] = err
        if 'reason' in params and reason is not None:
            kwargs['reason'] = reason
        if 'details' in params and details is not None:
            kwargs['details'] = details

        if kwargs:
            return method(test, **kwargs)

        if err is not None:
            return method(test, err)
        if reason is not None:
            return method(test, reason)
        return method(test)

    def startTest(self, test):
        test_id = _safe_test_id(test)
        primary_service = _guess_service(test.__class__.__module__)
        self._started[test_id] = datetime.now(timezone.utc)
        self._emitter.emit({
            'event_type': 'test_start',
            'test_id': test_id,
            'test_name': getattr(test, '_testMethodName', None),
            'test_class': test.__class__.__name__,
            'module': test.__class__.__module__,
            'primary_service_guess': primary_service,
            'test_layer_guess': _guess_layer(test.__class__.__module__),
        })
        return self._result.startTest(test)

    def stopTest(self, test):
        duration = None
        test_id = _safe_test_id(test)
        started = self._started.pop(test_id, None)
        if started is not None:
            duration = (datetime.now(timezone.utc) - started).total_seconds()
        self._emitter.emit({
            'event_type': 'test_stop',
            'test_id': test_id,
            'duration_sec': duration,
        })
        return self._result.stopTest(test)

    def addSuccess(self, test, details=None):
        self._emitter.emit({
            'event_type': 'test_success',
            'test_id': _safe_test_id(test),
            'status': 'success',
        })
        return self._call_result_method('addSuccess', test, details=details)

    def addError(self, test, err=None, details=None):
        test_id = _safe_test_id(test)
        primary_service = _guess_service(test.__class__.__module__)
        error_message = _extract_error_message(err=err, details=details)
        self._emitter.emit({
            'event_type': 'test_error',
            'test_id': test_id,
            'status': 'error',
            'primary_service_guess': primary_service,
            'test_layer_guess': _guess_layer(test.__class__.__module__),
            'error_message': error_message,
            'failure_signature': _failure_signature(
                test_id, error_message, primary_service=primary_service),
            'details': _extract_details(details),
        })
        return self._call_result_method('addError', test, err=err,
                                        details=details)

    def addFailure(self, test, err=None, details=None):
        test_id = _safe_test_id(test)
        primary_service = _guess_service(test.__class__.__module__)
        error_message = _extract_error_message(err=err, details=details)
        self._emitter.emit({
            'event_type': 'test_failure',
            'test_id': test_id,
            'status': 'failure',
            'primary_service_guess': primary_service,
            'test_layer_guess': _guess_layer(test.__class__.__module__),
            'error_message': error_message,
            'failure_signature': _failure_signature(
                test_id, error_message, primary_service=primary_service),
            'details': _extract_details(details),
        })
        return self._call_result_method('addFailure', test, err=err,
                                        details=details)

    def addSkip(self, test, reason=None, details=None):
        extracted_details = _extract_details(details)
        self._emitter.emit({
            'event_type': 'test_skip',
            'test_id': _safe_test_id(test),
            'status': 'skip',
            'skip_reason': reason or (extracted_details or {}).get('reason'),
            'details': extracted_details,
        })
        return self._call_result_method('addSkip', test, reason=reason,
                                        details=details)

    def addExpectedFailure(self, test, err=None, details=None):
        self._emitter.emit({
            'event_type': 'test_expected_failure',
            'test_id': _safe_test_id(test),
            'status': 'expected_failure',
            'error_message': _extract_error_message(err=err, details=details),
            'details': _extract_details(details),
        })
        return self._call_result_method('addExpectedFailure', test, err=err,
                                        details=details)

    def addUnexpectedSuccess(self, test, details=None):
        self._emitter.emit({
            'event_type': 'test_unexpected_success',
            'test_id': _safe_test_id(test),
            'status': 'unexpected_success',
            'details': _extract_details(details),
        })
        return self._call_result_method('addUnexpectedSuccess', test,
                                        details=details)
