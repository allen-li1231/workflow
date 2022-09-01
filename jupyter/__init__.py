import os
import base64
import urllib.parse
import json
import logging
from tqdm.auto import tqdm

from .base import JupyterBase
from .. import logger
from ..decorators import retry
from ..utils import read_file_in_chunks

__all__ = ["Jupyter"]


class Jupyter(JupyterBase):
    def __init__(self, password=None, verbose=False):
        super(Jupyter, self).__init__(password=password)
        self.log = logging.getLogger(__name__ + f".Jupyter")
        logger.set_log_level(self.log, verbose=verbose)

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

        if progressbar:
            file_size = os.path.getsize(file_path)
            pbar = tqdm(total=file_size,
                        desc=f"uploading {file_name}",
                        unit="iB",
                        unit_scale=True,
                        unit_divisor=1024,
                        position=progressbar_offset,
                        **self._progressbar_format)

        with open(file_path, 'rb') as f:
            for chunk, data in read_file_in_chunks(f, block_size=block_size):
                res = self._upload(data=data,
                                   file_name=file_name,
                                   dst_path=dst_path,
                                   chunk=chunk)
                if progressbar:
                    pbar.update(len(data))

        if progressbar:
            pbar.close()
        return res

    @retry(__name__)
    def _upload(self, data, file_name, dst_path, chunk):
        dst_url = urllib.parse.urljoin(self.base_url + "/api/contents/", dst_path)
        dst_url = dst_url + file_name if dst_url.endswith('/') else dst_url + '/' + file_name

        file_ext = file_name.rpartition('.')[-1]
        if file_ext == "ipynb":
            self.headers["Content-Type"] = "application/json"
            file_type = 'notebook'
        else:
            self.headers["Content-Type"] = "application/octet-stream"
            file_type = 'file'

        data = base64.b64encode(data).decode("utf-8") + '=' * (4 - len(data) % 4)
        body = json.dumps({
            "chunk": chunk,
            'content': data,
            'name': file_name,
            'path': dst_path,
            'format': 'base64',
            'type': file_type
        })
        res = self.put(dst_url, data=body)
        return res
