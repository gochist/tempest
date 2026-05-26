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
from datetime import datetime
from datetime import timezone
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
        self.run_id = os.getenv('TEMPEST_EVENT_RUN_ID') or _utcnow()
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
                headers={'Content-Type': 'application/json'},
                method='POST')
            try:
                with urllib.request.urlopen(request, timeout=1):
                    pass
            except urllib.error.URLError:
                LOG.exception('Failed to POST Tempest test event to %s', self.url)

        if not self.file_path and not self.url:
            print(payload, flush=True)


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

    def startTest(self, test):
        test_id = _safe_test_id(test)
        self._started[test_id] = datetime.now(timezone.utc)
        self._emitter.emit({
            'event_type': 'test_start',
            'test_id': test_id,
            'test_name': getattr(test, '_testMethodName', None),
            'test_class': test.__class__.__name__,
            'module': test.__class__.__module__,
            'primary_service_guess': _guess_service(test.__class__.__module__),
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
        return self._result.addSuccess(test, details=details)

    def addError(self, test, err=None, details=None):
        self._emitter.emit({
            'event_type': 'test_error',
            'test_id': _safe_test_id(test),
            'status': 'error',
            'error_message': _extract_error_message(err=err, details=details),
            'details': _extract_details(details),
        })
        return self._result.addError(test, err=err, details=details)

    def addFailure(self, test, err=None, details=None):
        self._emitter.emit({
            'event_type': 'test_failure',
            'test_id': _safe_test_id(test),
            'status': 'failure',
            'error_message': _extract_error_message(err=err, details=details),
            'details': _extract_details(details),
        })
        return self._result.addFailure(test, err=err, details=details)

    def addSkip(self, test, reason=None, details=None):
        self._emitter.emit({
            'event_type': 'test_skip',
            'test_id': _safe_test_id(test),
            'status': 'skip',
            'skip_reason': reason,
            'details': _extract_details(details),
        })
        return self._result.addSkip(test, reason=reason, details=details)

    def addExpectedFailure(self, test, err=None, details=None):
        self._emitter.emit({
            'event_type': 'test_expected_failure',
            'test_id': _safe_test_id(test),
            'status': 'expected_failure',
            'error_message': _extract_error_message(err=err, details=details),
            'details': _extract_details(details),
        })
        return self._result.addExpectedFailure(test, err=err, details=details)

    def addUnexpectedSuccess(self, test, details=None):
        self._emitter.emit({
            'event_type': 'test_unexpected_success',
            'test_id': _safe_test_id(test),
            'status': 'unexpected_success',
            'details': _extract_details(details),
        })
        return self._result.addUnexpectedSuccess(test, details=details)
