import time
import logging

from .base import ZeppelinBase, NoteBase, ParagraphBase
from .. import logger
from ..decorators import ensure_login
from ..settings import ZEPPELIN_INTERPRETER, ZEPPELIN_INACTIVE_TIME, ZEPPELIN_PARAGRAPH_CONFIG

__all__ = ["Zeppelin", "build_paragraph"]


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
    
    def _get_note_id_by_name(self, note_name: str):
        for note in self.list_notes():
            if note["name"] == note_name:
                return note["id"]

        raise FileNotFoundError("note name '{note_name}' does not exists")
        
    def _get_note_name_by_id(self, note_id: str):
        for note in self.list_notes():
            if note["id"] == note_id:
                return note["name"]

        raise FileNotFoundError("note id '{note_name}' does not exists")

    @ensure_login
    def list_notes(self):
        self.log.info("getting all notes")
        res = self._list_notes()
        r_json = res.json()
        if r_json["status"] != "OK":
            self.log.error(r_json.get("message", r_json))
        
        return r_json["body"]

    @ensure_login
    def get_note(self, note_name, note_id):
        if note_name is None and note_id is None:
            raise ValueError("either name of note or node id should be given")

        self.log.info("getting note")
        if note_name is None:
            note_name = self._get_note_name_by_id(note_id)
        if note_id is None:
            note_id = self._get_note_id_by_name(note_name)
        
        return Note(self, name=note_name, note_id=note_id)

    @ensure_login
    def create_note(self, name: str, paragraphs: None):
        self.log.info("creating note")
        res = self._create_note(name, paragraphs)
        r_json = res.json()
        if r_json["status"] != "OK":
            self.log.error(r_json.get("message", r_json))

        assert isinstance(r_json["body"], str)
        return Note(self, name, r_json["body"])

    @ensure_login
    def delete_note(self, note_name: str = None, note_id: str = None):
        if note_name is None and note_id is None:
            raise ValueError("either name of note or node id should be given")

        self.log.info("deleting note")
        if note_id:
            res = self._delete_note(note_id)
        else:
            # find note id w.r.t note name from list of all notes
            note_id = self.get_note_id_by_name(note_name)
            res = self._delete_note(note_id)

        r_json = res.json()
        if r_json["status"] != "OK":
            self.log.error(r_json.get("message", r_json))

    @ensure_login
    def import_note(self, note: dict):
        if not isinstance(note, dict) or "name" not in note:
            raise TypeError("incorrect note format, please use build_note to create a note")

        self.log.info("importing note")
        res = self._import_note(note)
        r_json = res.json()
        if r_json["status"] != "OK":
            self.log.error(r_json.get("message", r_json))

        assert isinstance(r_json["body"], str)
        return Note(self, note["name"], r_json["body"])
        
    @ensure_login
    def clone_note(self,
        new_note_name: str,
        note_name: str = None,
        note_id: str = None
    ):
        if note_name is None and note_id is None:
            raise ValueError("either name of original note or node id should be given")

        self.log.info("cloning note")
        if note_id:
            res = self._clone_note(note_id, new_note_name)
        else:
            # find note id w.r.t note name from list of all notes
            note_id = self.get_note_id_by_name(note_name=note_name)
            res = self._clone_note(note_id, new_note_name)

        r_json = res.json()
        if r_json["status"] != "OK":
            self.log.error(r_json.get("message", r_json))

        assert isinstance(r_json["body"], str)
        return Note(self, new_note_name, r_json["body"])

    @ensure_login
    def export_note(self,
        note_name: str = None,
        note_id: str = None,
        path: str = None):

        if note_name is None and note_id is None:
            raise ValueError("either name of original note or node id should be given")
    
        self.log.info("exporting note " + note_name or note_id)
        if note_id:
            res = self._export_note(note_id=note_id)
        else:
            note_id = self.get_note_id_by_name(note_name=note_name)
            res = self._export_note(note_id=note_id)

        if path:
            with open(path, mode="w", encoding="utf-8") as f:
                f.writelines(res.text)
        else:
            return res.json()


class Note(NoteBase):
    def __init__(self, zeppelin: Zeppelin, name: str, note_id: str):
        super().__init__(zeppelin, name, note_id)

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

    @ensure_login
    def run_all(self):
        res = self._run_all()


class Paragraph(ParagraphBase):
    def __init__(self, note: Note, paragraph_id: str):
        super().__init__(note, paragraph_id)

        self.zeppelin = note.zeppelin
        self.note = note
        # register method
        self.login = Zeppelin.login

    @property
    def _last_execute(self):
        return self.zeppelin._last_execute

    @_last_execute.setter
    def _last_execute(self, value):
        self.zeppelin._last_execute = value

    @property
    def is_logged_in(self):
        return self.zeppelin.is_logged_in


def build_note(note_name, paragraphs: list):
    assert isinstance(paragraphs, list)
    assert "text
    

def build_paragraph(text: str,
    title: None,
    config: ZEPPELIN_PARAGRAPH_CONFIG, 
    interpreter: ZEPPELIN_INTERPRETER
    ):
    paragraph = {}
    if title:
        paragraph["title"] = title

    if config:
        paragraph["config"] = config

    if interpreter:
        text = f"%{interpreter}\n" + text

    paragraph = {"text": text}
    return paragraph