import base64
import json
import logging
import os
import urllib.parse
import urllib.request

from tqdm.auto import tqdm

from .base import JupyterBase
from .. import logger
from ..decorators import retry
from ..utils import read_file_in_chunks

__all__ = ["Jupyter"]


class Jupyter(JupyterBase):
    def __init__(self, password=None, verbose=False):
        super(Jupyter, self).__init__(password=password, verbose=verbose)
        self.log = logging.getLogger(__name__ + f".Jupyter")
        logger.set_stream_log_level(self.log, verbose=verbose)

    def download(self, file_path, dst_path, progressbar=True, progressbar_offset=0):
        if not os.path.isdir(dst_path):
            raise NotADirectoryError(f"destination 'dst_path' does't exist or is not a directory")

        file_name = os.path.basename(file_path)
        buffer = self._download(file_path)
        if progressbar:
            setup_progressbar = self._progressbar_format.copy()
            setup_progressbar["bar_format"] = '{l_bar}{n_fmt}{unit}, {rate_fmt}{postfix} |{elapsed}'
            pbar = tqdm(total=None,
                        desc=f"downloading {file_name}",
                        unit="iB",
                        unit_scale=True,
                        unit_divisor=1024,
                        position=progressbar_offset,
                        **setup_progressbar)
        with open(dst_path, "wb") as f:
            for chunk in buffer.iter_content(chunk_size=8192):
                f.write(chunk)
                if progressbar:
                    pbar.update(len(chunk))

        if progressbar:
            pbar.close()
        return buffer.status_code

    def upload(self, file_path, dst_path, progressbar=True, progressbar_offset=0):
        """
        Uploads File to Jupyter Notebook Server
        ----------------------------------------

        :param file_path:
            The file path to the local content to be uploaded

        :param dst_path:
            The path where resource should be placed.
            The destination directory must exist.

        :param progressbar: whether to print progressbar during waiting
                          default to True

        :param progressbar_offset: use this parameter to control sql progressbar positions

        :return: server response
        """
        # default block size is 25MB
        block_size = self.max_upload_size
        file_name = os.path.basename(file_path)
        file_size = os.path.getsize(file_path)

        with open(file_path, 'rb') as f:
            if file_size <= block_size:
                res = self._upload(data=f.read(),
                                   file_name=file_name,
                                   dst_path=dst_path)
                r_json = res.json()
                if "message" in r_json:
                    raise RuntimeError(r_json["message"])
                return res.status_code

            if progressbar:
                pbar = tqdm(total=file_size,
                            desc=f"uploading {file_name}",
                            unit="iB",
                            unit_scale=True,
                            unit_divisor=1024,
                            position=progressbar_offset,
                            **self._progressbar_format)

            for chunk, data in read_file_in_chunks(f, block_size=block_size):
                res = self._upload(data=data,
                                   file_name=file_name,
                                   dst_path=dst_path,
                                   chunk=chunk)
                if progressbar:
                    pbar.update(len(data))

        if progressbar:
            pbar.close()
        return res.status_code

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
