import time
import logging

from .base import ZeppelinBase, NoteBase, ParagraphBase
from .. import logger
from ..decorators import ensure_login
from ..settings import ZEPPELIN_INACTIVE_TIME

__all__ = ["Zeppelin"]


class Zeppelin(ZeppelinBase):
    """
    Zeppelin API
    An intergraded Spark platform

    Parameters:
    username: str, default None
        Zeppelin username, if not provided here, user need to call login manually
    password: str, Hue password, default None
        Zeppelin password, if not provided here, user need to call login manually
    verbose: bool, default False
        whether to print log on stdout, default to False
    """

    def __init__(self,
                 username: str = None,
                 password: str = None,
                 verbose: bool = False):

        self.verbose = verbose
        self.log = logging.getLogger(__name__ + f".Zeppelin")
        if verbose:
            logger.set_stream_log_level(self.log, verbose=verbose)

        super(Zeppelin, self).__init__(username=username,
            password=password, verbose=verbose)

        if self.username is not None \
                and password is not None:
            self.login(self.username, password)

    @property
    def is_logged_in(self):
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
            print("Please provide Zeppelin password:", end='')
            self._password = input("")

        self.log.debug(f"logging in for user: [{self.username}]")
        res = self._login()
        if res.status_code != 200:
            self._password = None
            self.log.error('login failed for [%s] at %s'
                           % (self.username, self.base_url))
            raise ValueError('login failed for [%s] at %s'
                             % (self.username, self.base_url))
        else:
            self.log.info('login succeeful [%s] at %s'
                          % (self.username, self.base_url))

        return self


class Note(NoteBase):
    def __init__(self, zeppelin: Zeppelin, name: str, note_id: str):
        super().__init__(zeppelin, name, note_id)

        # register method
        self.login = zeppelin.login

    @property
    def _last_execute(self):
        return self.zeppelin._last_execute

    @property
    def is_logged_in(self):
        return self.zeppelin.is_logged_in


class Paragraph(ParagraphBase):
    def __init__(self, note: Note, paragraph_id: str):
        super().__init__(note, paragraph_id)

        # register method
        self.login = Zeppelin.login

    @property
    def _last_execute(self):
        return self.zeppelin._last_execute

    @property
    def is_logged_in(self):
        return self.zeppelin.is_logged_in
