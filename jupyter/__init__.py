import json
import logging
import os

from tqdm.auto import tqdm

from .base import JupyterBase
from .. import logger
from ..utils import read_file_in_chunks

__all__ = ["Jupyter"]


class Jupyter(JupyterBase):
    def __init__(self, password=None, verbose=False):
        super(Jupyter, self).__init__(password=password, verbose=verbose)
        self.log = logging.getLogger(__name__ + f".Jupyter")
        logger.set_stream_log_level(self.log, verbose=verbose)

        self.terminal = None

    def download(self, file_path, dst_path, progressbar=True, progressbar_offset=0):
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

    def new_terminal(self):
        return self._new_terminal().json()["name"]

    def close_terminal(self, name):
        res = self._close_terminal(name)
        if res.status_code != 204:
            raise RuntimeError(res.json()["message"])

    def get_terminals(self):
        return self._get_terminals().json()

    def create_terminal_connection(self, terminal_name):
        conn = self._ws_terminal(terminal_name)
        self.terminal = {"name": terminal_name, "ws": conn}
        return conn

    def execute_terminal(self, command, terminal_name=None, print_result=True):
        # initialize and move cursor to the end of terminal
        if self.terminal is None:
            terminal_name = self.new_terminal()
            conn = self.create_terminal_connection(terminal_name)
            while "setup" not in conn.recv():
                continue

        elif terminal_name:
            conn = self.create_terminal_connection(terminal_name)
            while "setup" not in conn.recv():
                continue
        else:
            conn = self.terminal["ws"]

        # execute command
        conn.send(json.dumps(["stdin", f"{command}\r"]))
        r_json = json.loads(conn.recv())
        # print input
        if print_result:
            print(r_json[1])

        # print output
        result = ""
        while not r_json[1].endswith("]$ "):
            r_json = json.loads(conn.recv())
            result += r_json[1]

        if print_result:
            print(result)

        return result

    def close(self):
        if self.terminal:
            self.terminal["ws"].close()
            self.close_terminal(self.terminal["name"])
