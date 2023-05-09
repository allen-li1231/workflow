import time
import logging

from .base import ZeppelinBase, NoteBase, ParagraphBase
from .. import logger
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

    def _get_note_id_by_name(self, note_name: str):
        for note in self.list_notes():
            if note["name"] == note_name:
                return note["id"]
        msg = f"note name '{note_name}' does not exists"
        self.log.warning(msg)
        raise FileNotFoundError(msg)
        
    def _get_note_name_by_id(self, note_id: str):
        for note in self.list_notes():
            if note["id"] == note_id:
                return note["name"]

        msg = "note id '{note_name}' does not exists"
        self.log.warning(msg)
        raise FileNotFoundError(msg)

    def list_notes(self):
        self.log.info("getting all notes")
        r_json = self._list_notes()
        return r_json["body"]

    def get_note(self, note_name, note_id):
        if note_name is None and note_id is None:
            raise ValueError("either name of note or node id should be given")

        self.log.info("getting note")
        if note_name is None:
            note_name = self._get_note_name_by_id(note_id)
        if note_id is None:
            note_id = self._get_note_id_by_name(note_name)
        
        return Note(self, name=note_name, note_id=note_id)

    def create_note(self, name: str, paragraphs: None):
        self.log.info("creating note")
        r_json = self._create_note(name, paragraphs)
        assert isinstance(r_json["body"], str)
        return Note(self, name, r_json["body"])

    def delete_note(self, note_name: str = None, note_id: str = None):
        if note_name is None and note_id is None:
            raise ValueError("either name of note or node id should be given")

        self.log.info("deleting note")
        if note_id:
            self._delete_note(note_id)
        else:
            # find note id w.r.t note name from list of all notes
            note_id = self.get_note_id_by_name(note_name)
            self._delete_note(note_id)

    def import_note(self, note: dict):
        if not isinstance(note, dict) or "name" not in note:
            raise TypeError("incorrect note format, please use build_note to create a note")

        self.log.info("importing note")
        r_json = self._import_note(note)
        assert isinstance(r_json["body"], str)
        return Note(self, note["name"], r_json["body"])
        
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
            r_json = self._clone_note(note_id, new_note_name)

        assert isinstance(r_json["body"], str)
        return Note(self, new_note_name, r_json["body"])

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
    def __init__(self,
        zeppelin: Zeppelin,
        name: str,
        note_id: str,
        verbose: bool = False):

        self.verbose = verbose
        self.log = logging.getLogger(__name__ + f".Note")
        if verbose:
            logger.set_stream_log_level(self.log, verbose=verbose)

        super().__init__(zeppelin, name, note_id)

    def get_paragraph(self, index):
        #TODO
        pass
    
    def run_all(self):
        res = self._run_all()


class Paragraph(ParagraphBase):
    def __init__(self,
        note: Note,
        paragraph_id: str,
        verbose: bool = False):

        self.verbose = verbose
        self.log = logging.getLogger(__name__ + f".Paragraph")
        if verbose:
            logger.set_stream_log_level(self.log, verbose=verbose)

        super().__init__(note, paragraph_id)

        self._cache = None

    @classmethod
    def build_paragraph(text: str,
        title: None,
        config: ZEPPELIN_PARAGRAPH_CONFIG,
        interpreter: ZEPPELIN_INTERPRETER):

        paragraph = {}
        if title:
            paragraph["title"] = title

        if config:
            paragraph["config"] = config

        if interpreter:
            text = f"%{interpreter}\n" + text

        paragraph = {"text": text}
        return paragraph

    @property
    def id(self):
        return self._paragraph_id

    @property
    def is_cached(self):
        return not self._cache is None

    @property
    def text(self):
        if self.is_cached:
            self.log.debug(f"get paragraph '{self.paragraph_id}' text from cache")
            return self._cache["text"]

        paragraph = self.get_info()
        return paragraph["text"]

    @text.setter
    def text(self, value):
        if not isinstance(value, str):
            raise TypeError("text value must be string")

        self.update(text=value)
        if self.is_cached:
            self._cache["text"] = value

    @property
    def title(self):
        if self.is_cached:
            self.log.debug(f"get paragraph '{self.paragraph_id}' title from cache")
            return self._cache["title"]

        paragraph = self.get_info()
        return paragraph.get("title", None)

    @title.setter
    def title(self, value):
        if not isinstance(value, str):
            raise TypeError("title value must be string")

        self.update(title=value)
        if self.is_cached:
            self._cache["title"] = value

    @property
    def date_updated(self):
        if self.is_cached:
            self.log.debug(f"get paragraph '{self.paragraph_id}' date_updated from cache")
            return self._cache["dateUpdated"]

        paragraph = self.get_info()
        return paragraph["dateUpdated"]

    @property
    def config(self):
        if self.is_cached:
            self.log.debug(f"get paragraph '{self.paragraph_id}' config from cache")
            return self._cache["config"]

        paragraph = self.get_info()
        return paragraph.get("config", None)

    @config.setter
    def config(self, value):
        if not isinstance(value, str):
            raise TypeError("config value must be string")
        
        self.update(config=value)
        if self.is_cached:
            self._cache["config"] = value

    @property
    def settings(self):
        if self.is_cached:
            self.log.debug(f"get paragraph '{self.paragraph_id}' settings from cache")
            return self._cache["settings"]

        paragraph = self.get_info()
        return paragraph.get("settings", None)

    @property
    def job_name(self):
        if self.is_cached:
            self.log.debug(f"get paragraph '{self.paragraph_id}' jobName from cache")
            return self._cache["jobName"]

        paragraph = self.get_info()
        return paragraph.get("jobName", None)

    @property
    def results(self):
        if self.is_cached:
            self.log.debug(f"get paragraph '{self.paragraph_id}' results from cache")
            return self._cache["results"]

        paragraph = self.get_info()
        return paragraph.get("results", None)

    @property
    def date_created(self):
        if self.is_cached:
            self.log.debug(f"get paragraph '{self.paragraph_id}' dateCreated from cache")
            return self._cache["dateCreated"]

        paragraph = self.get_info()
        return paragraph.get("dateCreated", None)

    @property
    def date_started(self):
        if self.is_cached:
            self.log.debug(f"get paragraph '{self.paragraph_id}' dateStarted from cache")
            return self._cache["dateStarted"]

        paragraph = self.get_info()
        return paragraph.get("dateStarted", None)

    @property
    def date_finished(self):
        if self.is_cached:
            self.log.debug(f"get paragraph '{self.paragraph_id}' dateFinished from cache")
            return self._cache["dateFinished"]

        paragraph = self.get_info()
        return paragraph.get("dateFinished", None)

    @property
    def status(self):
        if self.is_cached:
            self.log.debug(f"get paragraph '{self.paragraph_id}' status from cache")
            return self._cache["status"]

        paragraph = self.get_info()
        return paragraph.get("status", None)
    
    @property
    def progress_update_intervals(self):
        if self.is_cached:
            self.log.debug(f"get paragraph '{self.paragraph_id}' progressUpdateIntervalMs from cache")
            return self._cache["status"]

        paragraph = self.get_info()
        return paragraph.get("progressUpdateIntervalMs", None)

    def get_info(self):
        self.log.info(f"getting '{self._paragraph_id}' info")
        r_json = self._get_info()
        self._cache = r_json["body"]
        return r_json["body"]

    def get_status(self):
        self.log.info(f"getting '{self._paragraph_id}' status")
        r_json = self._get_status()
        self._cache = r_json["body"]
        return r_json["body"]
    
    def update_config(self, config: dict):
        if not isinstance(config, dict):
            err_msg = f"expect text as dict, got {type(config)}"
            self.log.error(err_msg)
            raise TypeError(err_msg)

        self.log.info(f"updating '{self._paragraph_id}' config with {config}")
        self._update_config(config)

    def update_text(self, text: str, title: str = None):
        if not isinstance(text, str):
            err_msg = f"expect text as str, got {type(text)}"
            self.log.error(err_msg)
            raise TypeError(err_msg)

        self.log.info(f"updating '{self._paragraph_id}' text")
        self._update_text(text, title=title)

    def update(self, **kwargs):
        if not self.is_cached and len(kwargs) == 0:
            self.log.warning("no argument to update, abort")
            return

        if len(kwargs) == 0:
            kwargs["text"] = self._cache["text"]
            if "title" in self._cache:
                kwargs["title"] = self._cache["title"]
            if "config" in self._cache:
                kwargs["config"] = self._cache["config"]

        if "config" in kwargs:
            self.update_config(kwargs["config"])
        if "text" in kwargs:
            self.update_text(kwargs["text"], title=kwargs.get("title", None))

    def drop(self):
        res = self._drop()
        r_json = res.json()
        if r_json["status"] != "OK":
            err_msg = r_json.get("message", r_json)
            self.log.error(err_msg)
            raise RuntimeError(err_msg)

def build_note(note_name, paragraphs: list):
    assert isinstance(paragraphs, list)
    
