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
from ..settings import PROGRESSBAR

JUPYTER_URL = 'http://10.19.181.26:9999'
JUPYTER_TOKEN = "fengkong"
MAX_UPLOAD_SIZE = 25 * 1024 * 1024


class JupyterBase(requests.Session):
    def __init__(self, password=None, verbose=False):
        super(JupyterBase, self).__init__()

        self.base_url = JUPYTER_URL
        self.token = JUPYTER_TOKEN
        self.max_upload_size = MAX_UPLOAD_SIZE

        self.log = logging.getLogger(__name__ + f".JupyterBase")
        logger.set_stream_log_level(self.log, verbose=verbose)

        setup_progressbar = PROGRESSBAR.copy()
        del setup_progressbar["desc"]
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
    def __init__(self, name, headers, cookies, verbose=False):
        self.base_url = JUPYTER_URL.replace("http", "ws") \
                        + f"/terminals/websocket/{name}?token={JUPYTER_TOKEN}"
        self.name = name

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

    def on_message(self, message):
        try:
            r_json = json.loads(message)
            source, message = r_json
            if source != "stdout":
                return
        except Exception as e:
            self.log.warning(e)
            self.log.warning(f"unable to parse and unpack'{message}' to json")
            message = message

        print(message)

    def on_error(self, error):
        warnings.warn(RuntimeError(error))

    def on_close(self, close_status_code, close_msg):
        print(f"### Terminal {self.name} closed ###")

    def on_open(self):
        print(f"### Opened terminal {self.name} connection ###")

    def execute(self, command):
        command = json.dumps(["stdin", f"{command}\r"])
        return super().send(command)

    def close(self, **kwargs):
        super(Terminal, self).close(**kwargs)
