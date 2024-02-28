import urllib.request
import urllib.parse
import requests
import websocket as ws
import base64
import random
import json
import logging
import warnings
from datetime import datetime

from .. import logger
from ..decorators import retry
from ..settings import PROGRESSBAR, JUPYTER_TOKEN, JUPYTER_URL, JUPYTER_MAX_UPLOAD_SIZE


class JupyterBase(requests.Session):
    def __init__(self, token=None, password=None, verbose=False):
        super(JupyterBase, self).__init__()

        self.base_url = JUPYTER_URL

        self.token = JUPYTER_TOKEN or '' if token is None else token
        self.max_upload_size = JUPYTER_MAX_UPLOAD_SIZE

        self.log = logging.getLogger(__name__ + f".JupyterBase")
        logger.set_stream_log_level(self.log, verbose=verbose)

        setup_progressbar = PROGRESSBAR.copy()
        if "desc" in setup_progressbar:
            del setup_progressbar["desc"]
        setup_progressbar["bar_format"] = '{l_bar}{bar:25}|{n_fmt}/{total_fmt}{unit} [{elapsed}<{remaining}]'
        self._progressbar_format = setup_progressbar

        self.headers["User-Agent"] = \
            "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.100 Safari/537.36"
        self.headers["X-Requested-With"] = "XMLHttpRequest"
        self.log.info(f"Jupyter logging in [{self.base_url}]")
        res = self.get(JUPYTER_URL + "/?token=" + JUPYTER_TOKEN)
        self.headers["X-XSRFToken"] = res.cookies["_xsrf"]

        if password is not None:
            # Seems jupyter auth process has changed, need to first get a cookie,
            # then add that cookie to the data being sent over with the password
            data = {"password": password}
            data.update(self.cookies)
            self.post(JUPYTER_URL + "/login", data=data)

        self.log.info(f"Jupyter login successful")

    @retry(__name__)
    def _get_sessions(self):
        url = self.base_url + "/api/sessions"
        res = self.get(url)
        return res

    @retry(__name__)
    def _get_session_info(self, session_id):
        url = self.base_url + f"/api/sessions/{session_id}"
        res = self.get(url)
        return res

    @retry(__name__)
    def _close_session(self, session_id):
        url = self.base_url + f"/api/sessions/{session_id}"
        res = self.delete(url)
        return res

    @retry(__name__)
    def _get_terminals(self):
        url = self.base_url + f"/api/terminals?{int(datetime.now().timestamp() * 10 ** 3)}"
        res = self.get(url)
        return res

    @retry(__name__)
    def _new_terminal(self):
        url = self.base_url + f"/api/terminals?{int(datetime.now().timestamp() * 10 ** 3)}"
        self.headers["Content-Type"] = "application/json"
        self.headers["Authorization"] = f"token {self.token}"
        res = self.post(url)
        return res

    @retry(__name__)
    def _close_terminal(self, name):
        url = self.base_url + f"/api/terminals/{name}?{int(datetime.now().timestamp() * 10 ** 3)}"
        self.headers["Authorization"] = f"token {self.token}"
        res = self.delete(url)
        return res

    @retry(__name__)
    def _download(self, file_path):
        url = urllib.parse.urljoin(self.base_url + "/files/",
                                   urllib.request.pathname2url(file_path))
        res = self.get(url, data={"download": 1}, stream=True)
        return res

    @retry(__name__)
    def _upload(self, data, file_name, dst_path, chunk=None):
        dst_url = urllib.parse.urljoin(self.base_url + "/api/contents/", dst_path)
        dst_url = dst_url + file_name if dst_url.endswith('/') else dst_url + '/' + file_name

        self.headers["Content-Type"] = "application/octet-stream"
        data = base64.b64encode(data).decode("utf-8") + '=' * (4 - len(data) % 4)
        body = {
            'content': data,
            'name': file_name,
            'path': dst_path,
            'format': 'base64',
            'type': 'file'
        }
        if chunk is not None:
            body["chunk"] = chunk

        res = self.put(dst_url, data=json.dumps(body))
        return res


class Terminal(ws.WebSocketApp):
    def __init__(self,
                 name,
                 headers,
                 cookies,
                 print_message=True,
                 verbose=False):
        self.base_url = JUPYTER_URL.replace("http", "ws") \
                        + f"/terminals/websocket/{name}?token={JUPYTER_TOKEN}"
        self.name = name
        self.print_message = print_message

        self.headers = {
            "Accept-Encoding": "gzip, deflate",
            "User-Agent": headers["User-Agent"],
            "Cache-Control": "no-cache",
            "Sec-WebSocket-Extensions": "permessage-deflate; client_max_window_bits",
            "Sec-WebSocket-Version": '13',
            "Sec-WebSocket-Key": str(base64.b64encode(bytes([random.randint(0, 255) for _ in range(16)])),
                                     'ascii'),
        }
        cookies = cookies.get_dict()
        self.cookies = "; ".join([f"{k}={v}" for k, v in cookies.items()])

        self.log = logging.getLogger(__name__ + f".Terminal")
        logger.set_stream_log_level(self.log, verbose=verbose)

        self.log.debug(f"initializing Terminal {name}")
        super().__init__(self.base_url,
                         header=self.headers,
                         cookie=self.cookies,
                         on_open=self.on_open,
                         on_message=self.on_message,
                         on_error=self.on_error,
                         on_close=self.on_close)
        self.msg = None

    def on_message(self, *args):
        def wrapper(message):
            try:
                r_json = json.loads(message)
                source, message = r_json
                if source != "stdout":
                    return
            except Exception as e:
                self.log.warning(e)
                self.log.warning(f"unable to parse and unpack'{message}' to json")
                message = message

            message = message.rstrip("\r\n")
            if "]$ " not in message:
                self.msg = message

            if self.print_message:
                print(message)

        if ws.__version__ >= "0.58.0":
            return wrapper(args[1])
        else:
            return wrapper(args[0])

    def on_error(self, *args):
        if ws.__version__ >= "0.58.0":
            warnings.warn(RuntimeError(args[1]))
        else:
            warnings.warn(RuntimeError(args[0]))

    def on_close(self, *args):
        if self.print_message:
            print(f"### Terminal {self.name} closed ###")

    def on_open(self, *args):
        if self.print_message:
            print(f"### Opened terminal {self.name} connection ###")

    def execute(self, command):
        command = json.dumps(["stdin", f"{command}\r"])
        return self.send(command)
