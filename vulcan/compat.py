import sys
import time
import logging
from decimal import Decimal
from threading import Thread, Event

from impala import dbapi, hiveserver2 as hs2
from impala.error import OperationalError
from impala._thrift_gen.TCLIService.ttypes import TGetOperationStatusReq, TOperationState

from ..logger import set_stream_log_level

_in_old_env = (sys.version_info.major <= 2) or (sys.version_info.minor <= 7)


class HiveServer2CompatCursor(hs2.HiveServer2Cursor):

    def __init__(self, host='localhost', port=21050, user=None, password=None, database=None,
                 config=None, verbose=False, timeout=None, use_ssl=False, ca_cert=None,
                 kerberos_service_name='impala', auth_mechanism='NOSASL', krb_host=None,
                 use_http_transport=False, http_path=''
                 ):

        self.log = logging.getLogger(__name__ + f".HiveServer2CompatCursor")
        set_stream_log_level(self.log, verbose=verbose)

        self.config = config

        self.conn = dbapi.connect(
            host, port, database=database, user=user, password=password, timeout=timeout, 
            use_ssl=use_ssl, ca_cert=ca_cert, auth_mechanism=auth_mechanism,
            kerberos_service_name=kerberos_service_name, krb_host=krb_host,
            use_http_transport=use_http_transport, http_path=http_path
        )
        session = self.conn.service.open_session(user, config)

        hs2.log.debug('HiveServer2Cursor(service=%s, session_handle=%s, '
                  'default_config=%s, hs2_protocol_version=%s)',
                  self.conn.service, session.handle,
                  session.config, session.hs2_protocol_version)

        self.log.debug('Cursor initialize (Impala session)')

        super().__init__(session)

        if self.conn.default_db is not None:
            hs2.log.info('Using database %s as default', self.conn.default_db)
            self.execute('USE %s' % self.conn.default_db)

        # self._stop_event = Event()
        # self._keep_alive_thread = Thread(
        #     target=self._keep_alive, args=(self._stop_event,), daemon=True
        # )
        # self._keep_alive_thread.start()

    @classmethod
    def _format(cls, v):
        if isinstance(v, Decimal):
            if v == int(v):
                v = int(v)
            else:
                v = float(v)
        return v

    def fetchall(self, verbose=False):
        self._wait_to_finish(verbose=verbose)
        if not self.has_result_set:
            return []

        try:
            if _in_old_env:
                desc = self.description or []
                return [
                    dict(zip([col[0] for col in desc], map(self._format, row)))
                    for row in super().fetchall()
                ]
            else:
                return list(self)
        except StopIteration:
            return []

    def execute(self, operation, param=None, config=None, verbose=True):
        """Synchronously execute a SQL query.

        Blocks until results are available.

        Parameters
        ----------
        operation : str
            The SQL query to execute.
        parameters : str, optional
            Parameters to be bound to variables in the SQL query, if any.
            Impyla supports all DB API `paramstyle`s, including `qmark`,
            `numeric`, `named`, `format`, `pyformat`.
        configuration : dict of str keys and values, optional
            Configuration overlay for this query.

        Returns
        -------
        NoneType
            Results are available through a call to `fetch*`.
        """
        # PEP 249
        self.execute_async(operation, parameters=param, configuration=config)
        self.log.debug('Waiting for query to finish')
        self._wait_to_finish(verbose=verbose)  # make execute synchronous
        self.log.debug('Query finished')

    def _check_operation_status(self, verbose=False):
        req = TGetOperationStatusReq(operationHandle=self._last_operation.handle)

        if _in_old_env:
            resp = self._last_operation._rpc('GetOperationStatus', req, True)
        else:
            resp = self._last_operation._rpc('GetOperationStatus', req)
        self._last_operation.update_has_result_set(resp)
        operation_state = TOperationState._VALUES_TO_NAMES[resp.operationState]
        if verbose:
            log = self.get_log()
            log.strip() and print(log)

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
            if _in_old_env:
                self._last_operation_finished = True
            return True

        return False

    def _wait_to_finish(self, verbose=False):
        # Prior to IMPALA-1633 GetOperationStatus does not populate errorMessage
        # in case of failure. If not populated, queries that return results
        # can get a failure description with a further call to FetchResults rpc.
        if _in_old_env and self._last_operation_finished:
            return

        loop_start = time.time()
        while True:
            is_finised = self._check_operation_status(verbose=verbose)
            if is_finised:
                break

            time.sleep(self._get_sleep_interval(loop_start))

    def _keep_alive(self, event, timeout=60.):
        while not event.is_set():
            try:
                self.session.ping()
                time.sleep(timeout)
            except Exception as e:
                self.log.exception(e)
                return

    def close(self):
        # stop keep-alive thread
        # self._stop_event.set()
        # close cursor
        super().close()
        self.session.close()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __enter__(self):
        return self
