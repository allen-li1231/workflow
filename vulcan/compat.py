import sys
import time
import logging
from decimal import Decimal
from impala import dbapi, hiveserver2 as hs2
from impala.error import OperationalError
from impala._thrift_gen.TCLIService.ttypes import TGetOperationStatusReq, TOperationState

from ..logger import set_stream_log_level
from ..settings import MAX_LEN_PRINT_SQL

_in_old_env = (sys.version_info.major <= 2) or (sys.version_info.minor <= 7)


class HiveServer2CompatCursor(hs2.HiveServer2Cursor):

    def __init__(self, host='localhost', port=21050, user=None, password=None, database=None,
                 config=None, verbose=False, timeout=None, use_ssl=False, ca_cert=None,
                 kerberos_service_name='impala', auth_mechanism='NOSASL', krb_host=None,
                 use_http_transport=False, http_path='', HS2connection=None
                 ):

        self.log = logging.getLogger(__name__ + f".HiveServer2CompatCursor")
        set_stream_log_level(self.log, verbose=verbose)

        self.user = user
        self.config = config
        self.verbose=verbose

        if HS2connection is None:
            self.log.debug(f"Connecting to '{host}:{port}'")
            HS2connection = dbapi.connect(
                host, port, database=database, user=user, password=password, timeout=timeout, 
                use_ssl=use_ssl, ca_cert=ca_cert, auth_mechanism=auth_mechanism,
                kerberos_service_name=kerberos_service_name, krb_host=krb_host,
                use_http_transport=use_http_transport, http_path=http_path
            )
        self.conn = HS2connection

        self.log.debug(f"Opening HS2 session for [{user}]")
        session = self.conn.service.open_session(user, config)

        hs2.log.debug('HiveServer2Cursor(service=%s, session_handle=%s, '
                'default_config=%s, hs2_protocol_version=%s)',
                self.conn.service, session.handle,
                session.config, session.hs2_protocol_version)

        self.log.debug('Cursor initialize')
        super().__init__(session)

        if self.conn.default_db is not None:
            hs2.log.info('Using database %s as default', self.conn.default_db)
            self.execute('USE %s' % self.conn.default_db)

    def _truncate_query_string(self, query_string):
        if query_string is None:
            return ''

        if len(query_string) <= MAX_LEN_PRINT_SQL:
            return query_string

        return query_string[:MAX_LEN_PRINT_SQL] + "..."

    @classmethod
    def _format(cls, v):
        if isinstance(v, Decimal):
            if v == int(v):
                v = int(v)
            else:
                v = float(v)
        return v
    
    def copy(self):
        self.log.debug("Make self a copy")
        return HiveServer2CompatCursor(HS2connection=self.conn)

    def fetchall(self, verbose=None):
        verbose = verbose if isinstance(verbose, bool) else self.verbose
        self._wait_to_finish(verbose=verbose)
        if not self.has_result_set:
            return []

        truncated_operation = self._truncate_query_string(self.query_string)
        self.log.debug(f"Fetchall result rows for '{truncated_operation}'")
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

    def execute(self, operation, param=None, config=None, verbose=None):
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
        truncated_operation = self._truncate_query_string(operation)
        self.log.debug(f"Fetchall result rows for '{truncated_operation}'")

        verbose = verbose if isinstance(verbose, bool) else self.verbose
        self.execute_async(operation, parameters=param, configuration=config)
        self._wait_to_finish(verbose=verbose)  # make execute synchronous

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
        self.log.debug('Waiting for query to finish')
        # Prior to IMPALA-1633 GetOperationStatus does not populate errorMessage
        # in case of failure. If not populated, queries that return results
        # can get a failure description with a further call to FetchResults rpc.
        if _in_old_env and self._last_operation_finished:
            self.log.debug('Query finished')
            return

        loop_start = time.time()
        while True:
            is_finised = self._check_operation_status(verbose=verbose)
            if is_finised:
                break

            time.sleep(self._get_sleep_interval(loop_start))

        self.log.debug('Query finished')

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __enter__(self):
        return self
