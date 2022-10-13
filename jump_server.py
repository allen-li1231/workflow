import time
import sys
import logging
import cx_Oracle
import paramiko
import threading

from . import logger
from .settings import (JUMP_SERVER_HOST, JUMP_SERVER_PORT, JUMP_SERVER_BACKEND_HOST,
                       JUMP_SERVER_PLSQL_HOST, JUMP_SERVER_PLSQL_SERVICE_NAME,
                       MAX_LEN_PRINT_SQL)

paramiko.util.log_to_file(logger.log_file, level=logging.DEBUG)


class SSH(paramiko.SSHClient):
    def __init__(self,
                 username, password,
                 jump_server_username, jump_server_password,
                 host=None, port=None,
                 file=sys.stdout, verbose=False):

        self.host = host or JUMP_SERVER_HOST
        self.port = port or JUMP_SERVER_PORT
        self.username = username
        self.jump_server_username = jump_server_username
        self.msg = ''
        self.file = file
        self.verbose = verbose
        self.log = logging.getLogger(__name__ + ".SSH")
        if self.verbose:
            logger.set_stream_log_level(self.log, verbose=self.verbose)

        super().__init__()
        self._login(self.username, password, self.jump_server_username, jump_server_password)

    def _login(self, username, password, jump_server_username, jump_server_password):
        self.load_system_host_keys()
        self.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        self.log.info("connect to jump server")
        self.connect(self.host, self.port,
                     username=jump_server_username,
                     password=jump_server_password)
        self.shell = self.invoke_shell()
        self.shell.set_combine_stderr(True)

        self.log.info(f"logging in for [{username}] on {self.host}")
        self.shell.send(f"1\r")
        time.sleep(1)
        self.shell.send(f"1\r")
        self.shell.send(f"{username}\r")

        self.log.info("start receiver output thread")
        self.print_thread = threading.Thread(target=self.print_forever, args=())
        self.print_thread.setDaemon(True)
        self.print_thread.start()

        time.sleep(1)
        self.shell.send(f"{password}\r")

    def print_forever(self, wait=0.5):
        this = threading.currentThread()
        while getattr(this, "keep_running", True):
            msg = self.shell.recv(-1).decode()
            if len(msg.strip()) > 0:
                print(msg, file=self.file, end='')
                self.msg = msg
            if "auto-logout" in msg:
                break

            time.sleep(wait)

        self.log.debug("print_forever joined")

    def execute(self, command):
        self.log.debug(f"execute shell command: {command}")
        self.shell.send(f"{command}\r")

    def close(self):
        self.log.debug(f"close print thread and do logout")
        self.shell.send("logout\r")
        self.shell.send("exit\r")
        self.print_thread.keep_running = False
        self.print_thread.join()
        super().close()


class SFTP(paramiko.SFTPClient):
    def __init__(self,
                 username, password,
                 jump_server_username, jump_server_password,
                 host=None, port=None,
                 verbose=False):
        self.host = host or JUMP_SERVER_HOST
        self.port = port or JUMP_SERVER_PORT
        self.username = username
        self.jump_server_username = jump_server_username
        self.verbose = verbose
        self.log = logging.getLogger(__name__ + ".SFTP")

        if self.verbose:
            logger.set_stream_log_level(self.log, verbose=self.verbose)

        self.log.info(f"logging in for [{username}] on {self.host}")
        t = paramiko.Transport(
            sock=(self.host, self.port)
        )
        t.connect(
            username=f"{jump_server_username}#{username}#{JUMP_SERVER_BACKEND_HOST}_sftp",
            password=f"{jump_server_password}#{password}"
        )
        chan = t.open_session()
        chan.invoke_subsystem("sftp")
        super().__init__(chan)

    def put(self, localpath, remotepath, callback=None, confirm=True):
        self.log.info(f"put '{localpath}' to '{remotepath}")
        super().put(localpath=localpath, remotepath=remotepath, callback=callback, confirm=confirm)

    def get(self, remotepath, localpath, callback=None):
        self.log.info(f"get '{localpath}' from '{remotepath}")
        super().get(remotepath=remotepath, localpath=localpath, callback=callback)


class Oracle:
    def __init__(self, username, password,
                 service_name=None, hostname=None,
                 verbose=False):

        self.verbose = verbose
        self.log = logging.getLogger(__name__ + ".Oracle")
        if self.verbose:
            logger.set_stream_log_level(self.log, verbose=self.verbose)

        """ Connect to the database. """
        self.hostname = hostname or JUMP_SERVER_PLSQL_HOST \
                        or ValueError("hostname not provided in argument or in settings")
        self.service_name = service_name or JUMP_SERVER_PLSQL_SERVICE_NAME \
                            or ValueError("service_name not provided in argument or in settings")
        try:
            service = self.hostname + '/' + self.service_name
            self.log.info(f"connect PL/SQL server for [{username}] on {service}")
            self.db = cx_Oracle.connect(username, password, service)
            self.db.autocommit = True
        except cx_Oracle.DatabaseError as e:
            self.log.exception(e)
            raise e
        # If the database connection succeeded create the cursor
        # we-re going to use.
        self.cursor = self.db.cursor()

    def close(self):
        """
        Disconnect from the database. If this fails, for instance
        if the connection instance doesn't exist, ignore the exception.
        """
        try:
            self.cursor.close()
            self.db.close()
        except cx_Oracle.DatabaseError as e:
            self.log.exception(e)
            pass

    def execute(self, sql):
        """
        Execute whatever SQL statements are passed to the method;
        commit if specified. Do not specify fetchall() in here as
        the SQL statement may not be a select.
        """
        try:
            self.log.info(f"execute sql: {sql[:MAX_LEN_PRINT_SQL]}")
            self.cursor.execute(sql)
        except cx_Oracle.DatabaseError as e:
            # Log error as appropriate
            self.log.exception(e)
            raise e

    def execute_proc(self, sql):
        """
        Execute whatever SQL procedure are passed to the method;
        commit if specified.
        """
        try:
            self.log.info(f"execute procedure: {sql[:MAX_LEN_PRINT_SQL]}")
            self.cursor.callproc(sql)
        except cx_Oracle.DatabaseError as e:
            # Log error as appropriate
            self.log.exception(e)
            raise e

    def fetchall(self):
        data = self.cursor.fetchall()
        col_names = []
        for i in range(0, len(self.cursor.description)):
            col_names.append(self.cursor.description[i][0])

        return {"data": data, "columns": col_names}
