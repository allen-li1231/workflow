import requests
from ..settings import PROGRESSBAR

JUPYTER_URL = 'http://10.19.181.26:9999'
TOKEN = "fengkong"
MAX_UPLOAD_SIZE = 25 * 1024 * 1024


class JupyterBase(requests.Session):
    def __init__(self, password=None):
        super(JupyterBase, self).__init__()
        self.base_url = JUPYTER_URL
        self.token = TOKEN
        self.max_upload_size = MAX_UPLOAD_SIZE

        setup_progressbar = PROGRESSBAR.copy()
        del setup_progressbar["desc"]
        self._progressbar_format = setup_progressbar

        self.headers[
            "User-Agent"] = "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.100 Safari/537.36"
        self.headers["X-Requested-With"] = "XMLHttpRequest"
        res = self.get(JUPYTER_URL + "/?token=" + TOKEN)
        self.headers["X-XSRFToken"] = res.cookies["_xsrf"]

        if password is not None:
            # Seems jupyter auth process has changed, need to first get a cookie,
            # then add that cookie to the data being sent over with the password
            data = {"password": password}
            data.update(self.cookies)
            self.post(JUPYTER_URL + "/login", data=data)
