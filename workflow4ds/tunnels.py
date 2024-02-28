import time
import sys
import logging
import paramiko
from paramiko.config import SSH_PORT
import threading

from . import logger

paramiko.util.log_to_file(logger.log_file, level=logging.INFO)


class SFTP(paramiko.SFTPClient):
    def __init__(self, username, password, host, port=None, verbose=False):
        self.username = username
        self._password = password
        self.host = host
        self.port = port or 22
        self.verbose = verbose

        self.log = logging.getLogger(__name__ + "SFTP")
        if self.verbose:
            logger.set_stream_log_level(self.log, verbose=self.verbose)

        self.transport = paramiko.Transport((self.host, self.port))
        self.transport.connect(username=username, password=password)

        chan = self.transport.open_session()
        chan.invoke_subsystem("sftp")
        super().__init__(chan)


class SSH(paramiko.SSHClient):
    def __init__(self, username, password, host, port=SSH_PORT, file=sys.stdout, verbose=False):
        self.username = username
        self._password = password
        self.host = host
        self.port = port
        self.file = file
        self.verbose = verbose

        self.log = logging.getLogger(__name__ + "SFTP")
        if self.verbose:
            logger.set_stream_log_level(self.log, verbose=self.verbose)

        super().__init__()
        self.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.connect(hostname=host, port=port, username=username, password=password)

        self.shell = self.invoke_shell()

        self.print_thread = threading.Thread(target=self.print_forever, args=())
        self.print_thread.setDaemon(True)
        self.print_thread.start()

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
        self.shell.send(command +'\r')

    def interrupt(self):
        self.execute("\x03")

    def close(self):
        self.log.debug(f"close print thread and do logout")
        self.execute("logout")
        self.print_thread.keep_running = False
        self.print_thread.join()
        super().close()
