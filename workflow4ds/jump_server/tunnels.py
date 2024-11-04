import os
import time
import sys
import logging
import paramiko
import threading

from .. import logger
from ..settings import JUMP_SERVER_HOST, JUMP_SERVER_PORT, JUMP_SERVER_BACKEND_HOST

paramiko.util.log_to_file(logger.log_file, level=logging.INFO)


class Tunnel:
    def setup(self,
              service,
              jump_server_username,
              jump_server_password,
              host, port,
              verbose=False):
        self.host = host
        self.port = port
        self.jump_server_username = jump_server_username
        self._jump_server_password = jump_server_password
        self.verbose = verbose

        self.log = logging.getLogger(__name__ + f".{service}")
        if self.verbose:
            logger.set_stream_log_level(self.log, verbose=self.verbose)


class SSH(paramiko.SSHClient, Tunnel):
    def __init__(self,
                 username, password,
                 jump_server_username=None, jump_server_password=None,
                 host=None, port=None,
                 file=sys.stdout, verbose=False):

        self.host = host or JUMP_SERVER_HOST
        self.port = port or JUMP_SERVER_PORT
        self.username = username
        self.msg = ''
        self.file = file
        super().setup("SSH",
                      jump_server_username=jump_server_username,
                      jump_server_password=jump_server_password,
                      host=self.host,
                      port=self.port,
                      verbose=verbose)
        super().__init__()
        self._login(self.username, password, self.jump_server_username, jump_server_password)

    def _login(self, username, password, jump_server_username, jump_server_password):
        self.load_system_host_keys()
        self.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        if jump_server_username is None:
            self.log.info(f"logging in for [{username}] on {self.host}")
            self.connect(self.host, self.port,
                         username=username,
                         password=password)
        else:
            self.log.info("connecting to jump server")
            self.connect(self.host, self.port,
                         username=jump_server_username,
                         password=jump_server_password)

        self.shell = self.invoke_shell()
        self.shell.set_combine_stderr(True)

        if not jump_server_username is None:
            self.log.info(f"logging in for [{username}] on {self.host}")
            self.shell.send(f"1\r")
            time.sleep(1)
            self.shell.send(f"1\r")
            self.shell.send(f"{username}\r")

        self.log.info("start receiver output thread")
        self.print_thread = threading.Thread(target=self.print_forever, args=())
        self.print_thread.setDaemon(True)
        self.print_thread.start()

        if not jump_server_username is None:
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

    def interrupt(self):
        self.shell.send("\x03")

    def close(self):
        self.log.debug(f"close print thread and do logout")
        self.execute("logout")
        self.execute("exit")
        self.print_thread.keep_running = False
        self.print_thread.join()
        super().close()


class SFTP(paramiko.SFTPClient, Tunnel):
    def __init__(self,
                 username, password,
                 jump_server_username=None, jump_server_password=None,
                 host=None, port=None,
                 verbose=False):
        self.host = host or JUMP_SERVER_HOST
        self.port = port or JUMP_SERVER_PORT
        self.username = username
        super().setup("SFTP",
                      jump_server_username=jump_server_username,
                      jump_server_password=jump_server_password,
                      host=self.host,
                      port=self.port,
                      verbose=verbose)

        self.log.info(f"logging in for [{username}] on {self.host}")
        t = paramiko.Transport(
            sock=(self.host, self.port)
        )
        if jump_server_username is None:
            t.connect(username=username, password=password)
        else:
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

    def put_dir(self, source, target):
        ''' Uploads the contents of the source directory to the target path. The
            target directory needs to exists. All subdirectories in source are 
            created under target.
        '''
        for item in os.listdir(source):
            if os.path.isfile(os.path.join(source, item)):
                self.put(os.path.join(source, item), '%s/%s' % (target, item))
            else:
                self.mkdir('%s/%s' % (target, item), ignore_existing=True)
                self.put_dir(os.path.join(source, item), '%s/%s' % (target, item))

    def mkdir(self, path, mode=511, ignore_existing=False):
        ''' Augments mkdir by adding an option to not fail if the folder exists  '''
        try:
            super().mkdir(path, mode)
        except IOError:
            if ignore_existing:
                pass
            else:
                raise