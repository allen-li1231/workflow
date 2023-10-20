import sys
import time
import logging
from decimal import Decimal
from threading import Thread, Event

from impala import hiveserver2 as hs2
from impala._thrift_gen.TCLIService.ttypes import (
    TGetOperationStatusReq, TOperationState)
from impala.error import  OperationalError

from ..logger import set_stream_log_level
from ..settings import HIVE_DEFAULT_CONFIG


class HiveServer2CompatCursor(hs2.HiveServer2Cursor):

    def __init__(self, host, port, user, password=None, database: str = None,
                 config=None, verbose=False, timeout=None, use_ssl=False, ca_cert=None,
                 kerberos_service_name='impala', auth_mechanism=None, krb_host=None,
                 use_http_transport=False, http_path=''
                 ):

        self.log = logging.getLogger(__name__ + f".HiveServer2CompatCursor")
        set_stream_log_level(self.log, verbose=verbose)

        config = HIVE_DEFAULT_CONFIG if config is None else config
        self.config = config.copy()
        self._in_old_env = (sys.version_info.major < 3) or (sys.version_info.minor < 9)

        session = hs2.connect(
            host, port, user=user, password=password, timeout=timeout, 
            use_ssl=use_ssl, ca_cert=ca_cert, kerberos_service_name=kerberos_service_name,
            auth_mechanism=auth_mechanism, krb_host=krb_host, 
            use_http_transport=use_http_transport, http_path=http_path
        )
        self.service = hs2.HiveServer2Connection(session, default_db=database)

        self.log.debug('getting new session_handle')

        session = self.service.open_session(user, config)

        self.log.debug('HiveServer2Cursor(service=%s, session_handle=%s, '
                  'default_config=%s, hs2_protocol_version=%s)',
                  self.service, session.handle,
                  session.config, session.hs2_protocol_version)

        self._last_operation_time = time.perf_counter()

        self.log.debug('Cursor initialize (Impala session)')

        super(self).__init__(session)

        self._stop_event = Event()
        self._keep_alive_thread = Thread(
            target=self._keep_alive, args=(self._stop_event,), daemon=True
        )
        self._keep_alive_thread.start()

    @classmethod
    def dictfetchall(cls, cursor):
        "Returns all rows from a cursor as a dict"
        def _format(v):
            if isinstance(v, Decimal):
                if v == int(v):
                    v = int(v)
                else:
                    v = float(v)
            return v

        desc = cursor.description or []
        return [
            dict(zip([col[0] for col in desc], map(_format, row)))
            for row in cursor.fetchall()
        ]

    def fetchall(self):
        if self.has_result_set:
            if self._in_old_env:
                return self.dictfetchall(self)
            else:
                return super().fetchall()

        return []

    def _check_operation_status(self, verbose=False):
        req = TGetOperationStatusReq(operationHandle=self._last_operation.handle)
        resp = self._last_operation._rpc('GetOperationStatus', req, True)
        self._last_operation.update_has_result_set(resp)
        operation_state = TOperationState._VALUES_TO_NAMES[resp.operationState]
        if verbose:
            log = self.get_log()
            log.strip() and self.log.info(log)

        if self._op_state_is_error(operation_state):
            if resp.errorMessage:
                raise OperationalError(resp.errorMessage)
            else:
                if self.fetch_error and self.has_result_set:
                    self._last_operation_active = False
                    self._last_operation.fetch()
                else:
                    raise OperationalError("Operation is in ERROR_STATE")

        if not self._op_state_is_executing(operation_state):
            if self._in_old_env:
                self._last_operation_finished = True
            return True

        return False

    def _wait_to_finish(self, verbose=False):
        # Prior to IMPALA-1633 GetOperationStatus does not populate errorMessage
        # in case of failure. If not populated, queries that return results
        # can get a failure description with a further call to FetchResults rpc.
        if self._in_old_env and self._last_operation_finished:
            return

        loop_start = time.time()
        while True:
            is_finised = self._check_operation_status(verbose=verbose)
            if is_finised:
                break

            time.sleep(self._get_sleep_interval(loop_start))

    def _keep_alive(self, event, timeout=30.):
        while not event.is_set():
            self.service.ping()
            time.sleep(timeout)

    def close(self):
        # stop keep-alive thread
        self._stop_event.set()
        # close cursor
        super().close()
        self.service.close()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __enter__(self):
        return self
