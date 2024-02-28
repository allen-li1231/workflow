import sys
import time
import logging
from decimal import Decimal
from impala import dbapi, hiveserver2 as hs2
from impala.error import OperationalError, HiveServer2Error
from impala._thrift_gen.TCLIService.ttypes import TGetOperationStatusReq, TOperationState

from ..logger import set_stream_log_level, set_log_path
from ..settings import MAX_LEN_PRINT_SQL

_in_old_env = (sys.version_info.major <= 2) or (sys.version_info.minor <= 7)


class HiveServer2CompatCursor(hs2.HiveServer2Cursor):

    def __init__(self, host='localhost', port=21050, user=None, password=None, database=None,
                 config=None, verbose=False, timeout=None, use_ssl=False, ca_cert=None,
                 kerberos_service_name='impala', auth_mechanism='NOSASL', krb_host=None,
                 use_http_transport=False, http_path='', name=None,
                 log_file_path=None, HS2connection=None
                 ):

        name = ".HiveServer2CompatCursor" if name is None else name
        self.log = logging.getLogger(__name__ + f".{name}")
        set_stream_log_level(self.log, verbose=verbose)
        if not log_file_path is None:
            set_log_path(self.log, log_file_path)

        self.user = user
        self.config = config
        self.verbose=verbose

        if not isinstance(HS2connection, hs2.HiveServer2Connection):
            self.log.debug(f"Connecting to HS2: '{host}:{port}'")
            HS2connection = dbapi.connect(
                host, port, database=database, user=user, password=password, timeout=timeout, 
                use_ssl=use_ssl, ca_cert=ca_cert, auth_mechanism=auth_mechanism,
                kerberos_service_name=kerberos_service_name, krb_host=krb_host,
                use_http_transport=use_http_transport, http_path=http_path
            )
        self.conn = HS2connection

        self._login(user, config)

    def _login(self, user, config):
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
    
    def copy(self, user=None, config=None, 
             name='HiveServer2CompatCursor', log_file_path=None
             ):
        self.log.debug("Make self a copy")
        return HiveServer2CompatCursor(
            user=user, config=config,
            HS2connection=self.conn, name=name, log_file_path=log_file_path
        )

    def _pop_from_buffer(self, size):
        self._ensure_buffer_is_filled()
        # put loggings into workflow's region
        self.log.debug(f'pop_from_buffer: popping {size} row out of buffer')
        return self._buffer.pop_many(size)
    
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
                local_buffer = []
                while True:
                    try:
                        elements = self._pop_from_buffer(self.buffersize)
                        local_buffer.extend(elements)
                    except StopIteration:
                        break

                return [
                    dict(zip([col[0] for col in desc], map(self._format, row)))
                    for row in local_buffer
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

    def execute_async(self, operation, parameters=None, configuration=None):
        try:
            return super().execute_async(operation, parameters, configuration)
        except HiveServer2Error as e:
            if str(e).startswith("Invalid SessionHandle"):
                self._login(self.user, self.config)
                return super().execute_async(operation, parameters, configuration)

    def _check_operation_status(self, verbose=False):
        req = TGetOperationStatusReq(operationHandle=self._last_operation.handle)

        if _in_old_env:
            resp = self._last_operation._rpc('GetOperationStatus', req, True)
        else:
            resp = self._last_operation._rpc('GetOperationStatus', req)

        self._last_operation.update_has_result_set(resp)
        operation_state = TOperationState._VALUES_TO_NAMES[resp.operationState]

        log = self.get_log()
        if len(log.strip()) >0:
            not self.verbose and verbose and print(log)
            self.log.info(log)

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
        self.log.info('Waiting for query to finish')
        # Prior to IMPALA-1633 GetOperationStatus does not populate errorMessage
        # in case of failure. If not populated, queries that return results
        # can get a failure description with a further call to FetchResults rpc.
        if _in_old_env and self._last_operation_finished:
            self.log.info('Query finished')
            return

        loop_start = time.time()
        try:
            while True:
                is_finised = self._check_operation_status(verbose=verbose)
                if is_finised:
                    break

                time.sleep(self._get_sleep_interval(loop_start))
        except KeyboardInterrupt:
            self.cancel_operation()

        self.log.info(f'Query finished in {time.time() - loop_start:.3f} secs')

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __enter__(self):
        return self

    def __next__(self):
        self._ensure_buffer_is_filled()
        # move loggings into workflow's region
        buffer = self._buffer.pop()
        self.log.debug(f'__next__: popping {len(buffer)} row out of buffer')
        return buffer