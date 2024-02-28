import time
import logging
import getpass
import requests

from .. import logger
from ..decorators import ensure_login, retry, handle_zeppelin_response
from ..settings import ZEPPELIN_URL, ZEPPELIN_INACTIVE_TIME, PROGRESSBAR


class ZeppelinBase(requests.Session):
    def __init__(self, username: str = None, password: str = None, verbose: bool = False):
        super(ZeppelinBase, self).__init__()

        self.username = username
        self._password = password
        self.verbose = verbose
        self.base_url = ZEPPELIN_URL
        self.log = logging.getLogger(__name__ + f".ZeppelinBase")
        logger.set_stream_log_level(self.log, verbose=verbose)

        self.headers["User-Agent"] = \
            "Mozilla/5.0 (X11; Linux x86_64; rv:99.0) Gecko/20100101 Firefox/99.0"

    @property
    def is_logged_in(self) -> bool:
        if not hasattr(self, "_last_execute"):
            return False

        return time.perf_counter() - self._last_execute < ZEPPELIN_INACTIVE_TIME

    def login(self, username: str = None, password: str = None):
        self.username = username or self.username
        self._password = password or self._password
        if self.username is None and self._password is None:
            raise ValueError("please provide username and password")

        if self.username is None and self._password is not None:
            raise KeyError("username must be specified with password")

        if self.username is not None and self._password is None:
            self._password = getpass.getpass("Please provide Zeppelin password: ")

        self.log.debug(f"logging in for user: [{self.username}]")
        res = self._login(username=self.username, password=self._password)
        if res.status_code != 200:
            self._password = None
            self.log.error('login failed for [%s] at %s'
                           % (self.username, self.base_url))
            raise ValueError('login failed for [%s] at %s'
                             % (self.username, self.base_url))
        else:
            self._last_execute = time.perf_counter()
            self.log.info('login succeeful [%s] at %s'
                          % (self.username, self.base_url))
        return self

    @retry(__name__)
    def _login(self, username, password):
        url = self.base_url + "/api/login"
        res = self.post(url, data={"userName": username, "password": password})
        return res

    @handle_zeppelin_response
    @ensure_login
    @retry(__name__)
    def _list_notes(self):
        url = self.base_url + "/api/notebook"
        res = self.get(url)
        return res

    @handle_zeppelin_response
    @ensure_login
    @retry(__name__)
    def _create_note(self, name: str, paragraphs: list):
        url = self.base_url + "/api/notebook"
        res = self.post(url, json={"name": name, "paragraphs": paragraphs})
        return res

    @handle_zeppelin_response
    @ensure_login
    @retry(__name__)
    def _delete_note(self, note_id):
        url = self.base_url + f"/api/notebook/{note_id}"
        res = self.delete(url)
        return res

    @handle_zeppelin_response
    @ensure_login
    @retry(__name__)
    def _import_note(self, note):
        url = self.base_url + "/api/notebook/import"
        res = self.post(url, json=note)
        return res

    @handle_zeppelin_response
    @ensure_login
    @retry(__name__)
    def _clone_note(self, note_id, name):
        url = self.base_url + f"/api/notebook/{note_id}"
        res = self.post(url, json={"name": name})
        return res

    @handle_zeppelin_response
    @ensure_login
    @retry(__name__)
    def _export_note(self, note_id):
        url = self.base_url + f"/api/notebook/export/{note_id}"
        res = self.get(url)
        return res


class NoteBase(requests.Session):
    def __init__(self, zeppelin: ZeppelinBase, name: str, note_id: str):
        super(NoteBase, self).__init__()

        self.name = name
        self.note_id = note_id
        self.base_url = zeppelin.base_url
        self.log = logging.getLogger(__name__ + f".NoteBase")
        if zeppelin.verbose:
            logger.set_stream_log_level(self.log, verbose=zeppelin.verbose)

        setup_progressbar = PROGRESSBAR.copy()
        setup_progressbar["desc"] = 'Zeppelin[{name}] awaiting {result}'
        setup_progressbar["bar_format"] = '{l_bar}{bar:25}|{n_fmt}/{total_fmt}'
        self._progressbar_format = setup_progressbar

        self.headers = zeppelin.headers
        self.cookies = zeppelin.cookies

        self.zeppelin = zeppelin
        # register method
        self.login = zeppelin.login

    @property
    def _last_execute(self):
        return self.zeppelin._last_execute

    @_last_execute.setter
    def _last_execute(self, value):
        self.zeppelin._last_execute = value

    @property
    def is_logged_in(self):
        return self.zeppelin.is_logged_in
    
    @handle_zeppelin_response
    @ensure_login
    @retry(__name__)
    def _get_all_status(self):
        url = self.base_url + f"/api/notebook/job/{self.note_id}"
        res = self.get(url)
        return res

    @handle_zeppelin_response
    @ensure_login
    @retry(__name__)
    def _get_info(self):
        url = self.base_url + f"/api/notebook/{self.note_id}"
        res = self.get(url)
        return res

    @handle_zeppelin_response
    @ensure_login
    @retry(__name__)
    def _delete_note(self):
        url = self.base_url + f"/api/notebook/{self.note_id}"
        res = self.delete(url)
        return res

    @handle_zeppelin_response
    @ensure_login
    @retry(__name__)
    def _clone_note(self, name):
        url = self.base_url + f"/api/notebook/{self.note_id}"
        res = self.post(url, json={"name": name})
        return res

    @handle_zeppelin_response
    @ensure_login
    @retry(__name__)
    def _export_note(self):
        url = self.base_url + f"/api/notebook/export/{self.note_id}"
        res = self.get(url)
        return res

    @handle_zeppelin_response
    @ensure_login
    @retry(__name__)
    def _import_note(self, note: dict):
        url = self.base_url + "/api/notebook/import"
        res = self.post(url, json=note)
        return res

    @handle_zeppelin_response
    @ensure_login
    @retry(__name__)
    def _run_all(self):
        url = self.base_url + f"/api/notebook/job/{self.note_id}"
        res = self.post(url)
        return res

    @handle_zeppelin_response
    @ensure_login
    @retry(__name__)
    def _stop_all(self):
        url = self.base_url + f"/api/notebook/job/{self.note_id}"
        res = self.delete(url)
        return res

    @handle_zeppelin_response
    @ensure_login
    @retry(__name__)
    def _clear_all_result(self):
        url = self.base_url + f"/api/notebook/{self.note_id}/clear"
        res = self.put(url)
        return res

    @handle_zeppelin_response
    @ensure_login
    @retry(__name__)
    def _create_paragraph(self, text: str, title=None, index: int = -1, config: dict = None):
        url = self.base_url + f"/api/notebook/{self.note_id}/paragraph"
        payload = {"text": text}
        # if index not given, add to last by default
        if index > -1:
            payload["index"] = index
        if title:
            payload["title"] = title
        if config:
            payload["config"] = config

        res = self.post(url, json=payload)
        return res

    @handle_zeppelin_response
    @ensure_login
    @retry(__name__)
    def _add_cron(self, cron: str, release_resource=False):
        url = self.base_url + f"/api/notebook/cron/{self.note_id}"
        res = self.post(url, json={"cron": cron, "releaseResource": release_resource})
        return res

    @handle_zeppelin_response
    @ensure_login
    @retry(__name__)
    def _remove_cron(self):
        url = self.base_url + f"/api/notebook/cron/{self.note_id}"
        res = self.delete(url)
        return res

    @handle_zeppelin_response
    @ensure_login
    @retry(__name__)
    def _get_cron(self):
        url = self.base_url + f"/api/notebook/cron/{self.note_id}"
        res = self.get(url)
        return res

    @handle_zeppelin_response
    @ensure_login
    @retry(__name__)
    def _get_permission(self):
        url = self.base_url + f"/api/notebook/{self.note_id}/permissions"
        res = self.get(url)
        return res

    @handle_zeppelin_response
    @ensure_login
    @retry(__name__)
    def _set_permission(self, readers: list, owners: list, runners: list, writers: list):
        url = self.base_url + f"/api/notebook/cron/{self.note_id}"
        payload = {
            "readers": readers,
            "owners": owners,
            "runners": runners,
            "writers": writers,
        }
        res = self.put(url, json=payload)
        return res


class ParagraphBase(requests.Session):
    def __init__(self, note: NoteBase, paragraph_id: str):
        super(ParagraphBase, self).__init__()

        self._paragraph_id = paragraph_id
        self.note_id = note.note_id
        self.base_url = note.base_url
        self.log = logging.getLogger(__name__ + f".ParagraphBase")
        if note.verbose:
            logger.set_stream_log_level(self.log, verbose=note.verbose)

        self._progressbar_format = note._progressbar_format

        self.headers = note.headers
        self.cookies = note.cookies

        self.zeppelin = note.zeppelin
        self.note = note
        # register method
        self.login = note.zeppelin.login

    @property
    def _last_execute(self):
        return self.zeppelin._last_execute

    @_last_execute.setter
    def _last_execute(self, value):
        self.zeppelin._last_execute = value

    @property
    def is_logged_in(self):
        return self.zeppelin.is_logged_in

    @handle_zeppelin_response
    @ensure_login
    @retry(__name__)
    def _get_info(self):
        url = self.base_url + f"/api/notebook/{self.note_id}/paragraph/{self.paragraph_id}"
        res = self.get(url)
        return res

    @handle_zeppelin_response
    @ensure_login
    @retry(__name__)
    def _get_status(self):
        url = self.base_url + f"/api/notebook/job/{self.note_id}/{self.paragraph_id}"
        res = self.get(url)
        return res

    @handle_zeppelin_response
    @ensure_login
    @retry(__name__)
    def _update_text(self, text: str, title=None):
        url = self.base_url + f"/api/notebook/{self.note_id}/paragraph/{self.paragraph_id}"
        payload = {"text": text}
        if title:
            payload["title"] = title

        res = self.put(url, json=payload)
        return res

    @handle_zeppelin_response
    @ensure_login
    @retry(__name__)
    def _update_config(self, config):
        url = self.base_url + f"/api/notebook/{self.note_id}/paragraph/{self.paragraph_id}/config"
        res = self.put(url, json=config)
        return res

    @handle_zeppelin_response
    @ensure_login
    @retry(__name__)
    def _delete(self):
        url = self.base_url + f"/api/notebook/{self.note_id}/paragraph/{self.paragraph_id}"
        res = self.delete(url)
        return res

    @handle_zeppelin_response
    @ensure_login
    @retry(__name__)
    def _run(self, sync=True, option: dict = None):
        if sync:
            url = self.base_url + f"/api/notebook/run/{self.note_id}/{self.paragraph_id}"
        else:
            url = self.base_url + f"/api/notebook/job/{self.note_id}/{self.paragraph_id}"

        res = self.post(url, json=option)
        return res

    @handle_zeppelin_response
    @ensure_login
    @retry(__name__)
    def _stop(self):
        url = self.base_url + f"/api/notebook/job/{self.note_id}/{self.paragraph_id}"
        res = self.delete(url)
        return res

    @handle_zeppelin_response
    @ensure_login
    @retry(__name__)
    def _move_to_index(self, index: int):
        url = self.base_url + f"/api/notebook/{self.note_id}/paragraph/{self.paragraph_id}/move/{index}"
        res = self.post(url)
        return res
