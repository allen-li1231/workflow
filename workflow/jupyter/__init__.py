import logging
import os
from threading import Thread
from tqdm import tqdm

from .base import JupyterBase, Terminal
from .. import logger
from ..utils import read_file_in_chunks

__all__ = ["Jupyter"]


class Jupyter(JupyterBase):
    def __init__(self, token=None, password=None, verbose=False):
        super(Jupyter, self).__init__(token=token, password=password, verbose=verbose)
        self.log = logging.getLogger(__name__ + f".Jupyter")
        logger.set_stream_log_level(self.log, verbose=verbose)

        self.terminal = None
        self.verbose = verbose

    def download(self, file_path, dst_path, progressbar=True, progressbar_offset=0):
        """
        Download File to Jupyter Notebook Server
        ----------------------------------------

        :param file_path:
            The file path to the Jupyter content to be downloaded

        :param dst_path:
            The path where resource should be placed in local.
            The destination directory must exist.

        :param progressbar: whether to print progressbar during waiting
                          default to True

        :param progressbar_offset: use this parameter to control sql progressbar positions

        :return: server response
        """

        self.log.debug(f"downloading '{file_path}' to '{dst_path}")
        if not os.path.isdir(dst_path):
            raise NotADirectoryError(f"destination '{dst_path}' does't exist or is not a directory")

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
        with open(os.path.join(dst_path, file_name), "wb") as f:
            for chunk in buffer.iter_content(chunk_size=8192):
                f.write(chunk)
                if progressbar:
                    pbar.update(len(chunk))

        if progressbar:
            pbar.close()
        return buffer

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
        self.log.debug(f"uploading '{file_path}' to '{dst_path}")

        with open(file_path, 'rb') as f:
            if file_size <= block_size:
                res = self._upload(data=f.read(),
                                   file_name=file_name,
                                   dst_path=dst_path)
                if res.status_code >= 400:
                    r_json = res.json()
                    if "message" in r_json:
                        raise RuntimeError(r_json["message"])
                    else:
                        raise RuntimeError(r_json)

                return res

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
                if res.status_code >= 400:
                    r_json = res.json()
                    if "message" in r_json:
                        raise RuntimeError(r_json["message"])
                    else:
                        raise RuntimeError(r_json)

                if progressbar:
                    pbar.update(len(data))

        if progressbar:
            pbar.close()
        return res

    def new_session(self):
        return self._new_terminal().json()["name"]

    def close_session(self, session_id):
        res = self._close_session(session_id=session_id)
        if res.status_code != 204:
            self.log.warning(res.text)

        return res

    def get_sessions(self):
        res = self._get_sessions()
        return res.json()

    def new_terminal(self):
        return self._new_terminal().json()["name"]

    def close_terminal(self, name=None):
        if name:
            if self.terminal and self.terminal["name"] == name:
                self.terminal["ws"].close()
                self.terminal["thread"].join()
                self.terminal = None

            res = self._close_terminal(name)
        elif self.terminal:
            self.terminal["ws"].close()
            self.terminal["thread"].join()
            res = self._close_terminal(self.terminal["name"])
            self.terminal = None
        else:
            raise ValueError("please specify terminal name")

        if res.status_code != 204:
            raise RuntimeError(res.json()["message"])

    def get_terminals(self):
        return self._get_terminals().json()

    def connect_terminal(self, name, print_message=True):
        self.log.debug(f"creating terminal {name} connection")
        conn = Terminal(name=name,
                        headers=self.headers,
                        cookies=self.cookies,
                        print_message=print_message,
                        verbose=self.verbose)
        thread = Thread(target=conn.run_forever, args=())
        self.terminal = {"name": name,
                         "ws": conn,
                         "thread": thread}
        thread.setDaemon(True)
        thread.start()
        return conn
