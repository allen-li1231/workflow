import urllib.request
import urllib.parse
import requests
import websocket as ws
import base64
import random
import json
import logging
from datetime import datetime
from .. import logger
from ..decorators import retry
from ..settings import PROGRESSBAR

JUPYTER_URL = 'http://10.19.181.26:9999'
TOKEN = "fengkong"
MAX_UPLOAD_SIZE = 25 * 1024 * 1024


class JupyterBase(requests.Session):
    def __init__(self, password=None, verbose=False):
        super(JupyterBase, self).__init__()

        self.base_url = JUPYTER_URL
        self.ws_url = self.base_url.replace("http", "ws")
        self.token = TOKEN
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
        res = self.get(JUPYTER_URL + "/?token=" + TOKEN)
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

    def _ws_terminal(self, name):
        headers = {
            "Accept-Encoding": "gzip, deflate",
            "User-Agent": self.headers["User-Agent"],
            "Cache-Control": "no-cache",
            "Sec-WebSocket-Extensions": "permessage-deflate; client_max_window_bits",
            "Sec-WebSocket-Version": '13',
            "Sec-WebSocket-Key": str(base64.b64encode(bytes([random.randint(0, 255) for _ in range(16)])),
                                     'ascii'),
        }
        cookies = self.cookies.get_dict()
        cookies = "; ".join([f"{k}={v}" for k, v in cookies.items()])

        url = self.ws_url + f"/terminals/websocket/{name}?token={self.token}"

        conn = ws.WebSocket(skip_utf8_validation=True)
        conn.connect(url, header=headers, cookie=cookies)
        return conn
