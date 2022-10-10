import time
import paramiko
from threading import Thread

from .settings import JUMP_SERVER_HOST, JUMP_SERVER_PORT, JUMP_SERVER_BACKEND_HOST


class SSH(paramiko.SSHClient):
    def __init__(self,
                 username, password,
                 jump_server_username, jump_server_password,
                 host=None, port=None):

        super().__init__()
        self.load_system_host_keys()
        self.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.connect(host or JUMP_SERVER_HOST, port or JUMP_SERVER_PORT,
                     username=jump_server_username, password=jump_server_password)
        self.shell = self.invoke_shell()

        self.msg = ''

        self.print_thread = Thread(target=self.print_forever, args=())
        self.print_thread.setDaemon(True)
        self.print_thread.start()

        self.execute(1)
        time.sleep(1)
        self.execute(1)
        self.execute(username)
        time.sleep(1)
        self.execute(password)

    def print_forever(self, wait=1):
        while True:
            msg = self.shell.recv(-1).decode()
            if len(msg.strip()) > 0:
                print(msg)
                self.msg = msg

            time.sleep(wait)

    def execute(self, command):
        self.shell.send(f"{command}\r")

    def close(self):
        self.print_thread.join()
        super().close()


class SFTP(paramiko.SFTPClient):
    def __init__(self,
                 username, password,
                 jump_server_username, jump_server_password,
                 host=None, port=None):
        t = paramiko.Transport(
            sock=(host or JUMP_SERVER_HOST, port or JUMP_SERVER_PORT)
        )
        t.connect(
            username=f"{jump_server_username}#{username}#{JUMP_SERVER_BACKEND_HOST}_sftp",
            password=f"{jump_server_password}#{password}"
        )
        chan = t.open_session()
        chan.invoke_subsystem("sftp")
        super().__init__(chan)
