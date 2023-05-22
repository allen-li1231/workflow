import re
import logging

from .base import ZeppelinBase, NoteBase, ParagraphBase
from .. import logger
from ..settings import ZEPPELIN_INTERPRETER, ZEPPELIN_PARAGRAPH_CONFIG

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

        super(Zeppelin, self).__init__(
            username=username,
            password=password,
            verbose=verbose)

        if self.username is not None \
                and password is not None:
            self.login(self.username, password)

    def _get_note_id_by_name(self, note_name: str):
        for note in self.list_notes():
            if note["name"].strip("/") == note_name.strip().strip("/"):
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
        return r_json

    def get_note(self, note_name: str = None, note_id: str = None):
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
        assert isinstance(r_json, str)
        return Note(self, name, r_json)

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

    def import_note(self, note: dict, verbose=False):
        if not isinstance(note, dict) \
            or "paragraphs" not in note \
            or "name" not in note:
            raise TypeError("wrong note format given, please make sure to use Note.build_note")

        self.log.info(f"importing note[{note['name']}]")
        r_json = self._import_note(note)
        return Note(self,
            name=note["name"],
            note_id=r_json,
            verbose=verbose)

    def import_py(self,
        path: str,
        note_name: str,
        verbose=False,
        config=ZEPPELIN_PARAGRAPH_CONFIG,
        interpreter=ZEPPELIN_INTERPRETER,
        **open_kwargs):

        if "mode" in open_kwargs:
            del open_kwargs["mode"]

        with open(path, mode='r', **open_kwargs) as f:
            text = f.read()

        d_note = Note.build_note(
            note_name=note_name,
            text=text,
            config=config,
            interpreter=interpreter)
        return self.import_note(note=d_note, verbose=verbose)

    def clone_note(self,
            new_note_name: str,
            note_name: str = None,
            note_id: str = None):

        if note_name is None and note_id is None:
            raise ValueError("either name of original note or node id should be given")

        self.log.info("cloning note")
        if note_id:
            r_json = self._clone_note(note_id, new_note_name)
        else:
            # find note id w.r.t note name from list of all notes
            note_id = self.get_note_id_by_name(note_name=note_name)
            r_json = self._clone_note(note_id, new_note_name)

        assert isinstance(r_json, str)
        return Note(self, new_note_name, r_json)

    def export_note(self,
            note_name: str = None,
            note_id: str = None,
            path: str = None):

        if note_name is None and note_id is None:
            raise ValueError("either name of original note or node id should be given")
    
        self.log.info(f"exporting note '{note_name or note_id}'")
        if note_id:
            r_json = self._export_note(note_id=note_id)
        else:
            note_id = self._get_note_id_by_name(note_name=note_name)
            r_json = self._export_note(note_id=note_id)

        if path:
            with open(path, mode="w", encoding="utf-8") as f:
                f.writelines(r_json)
        else:
            return r_json

    def export_py(self, 
            note_name: str = None,
            note_id: str = None,
            path: str = None,
            sep='\n'):

        if note_name is None and note_id is None:
            raise ValueError("either name of original note or node id should be given")

        self.log.info(f"exporting note[{self.name}] to python file")
        note = self.get_note(note_name=note_name, note_id=note_id)

        return note.export_py(path=path, sep=sep)


class Note(NoteBase):
    _regex_py_sep = re.compile(r"(?s)#+(%[\w\d_\.]+\s*\n+.*?)(?=\n\s*#+%[\w\d_\.]+|\Z)")

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
        self._regex_interpreters = re.compile(r"[\s\n]*(%[\w\d_\.]+\s*\n+)")

    @classmethod
    def build_note(cls,
            note_name: str,
            text: str = None,
            paragraphs: list = None,
            config: dict = ZEPPELIN_PARAGRAPH_CONFIG,
            interpreter: str = ZEPPELIN_INTERPRETER):

        assert isinstance(note_name, str)
        if text is None and paragraphs is None:
            raise ValueError("either text or paragraphs should be provided")

        assert isinstance(text, str) or isinstance(paragraphs, list)
        if isinstance(paragraphs, list):
            return {"name": note_name, "paragraphs": paragraphs}
        
        if len(text) == 0:
            return {"name": note_name, "paragraphs": []}

        lst_text = re.findall(Note._regex_py_sep, text)
        if len(lst_text) == 0:
            lst_text = [text]

        paragraphs = [
            Paragraph.build_paragraph(text=t, config=config, interpreter=interpreter)
            for t in lst_text
            if len(t) > 0]

        return {"name": note_name, "paragraphs": paragraphs}

    @property
    def info(self):
        self.log.info(f"getting note[{self.name}] info")
        r_json = self._get_info()
        return r_json

    def run_all(self):
        self.log.info(f"running note[{self.name}]")
        r_json = self._run_all()
        return r_json

    def stop_all(self):
        self.log.info(f"stoping note[{self.name}]")
        r_json = self._stop_all()
        return r_json

    def clear_all_result(self):
        self.log.info(f"clearing all result in note[{self.name}]")
        r_json = self._clear_all_result()
        return r_json
    
    def get_all_status(self):
        self.log.info(f"getting all paragraph status in note[{self.name}]")
        r_json = self._get_all_status()
        return r_json

    def delete(self):
        self.log.info(f"deleting note[{self.name}]")
        r_json = self._delete_note()
        return r_json

    def clone(self, name: str, verbose=False):
        self.log.info(f"cloning note[{self.name}]")
        r_json = self._clone_note(name=name)
        return Note(self.zeppelin,
            name=name,
            note_id=r_json,
            verbose=verbose)

    def export_note(self, path: str = None):
        self.log.info(f"exporting note[{self.name}]")
        r_json = self._export_note()
        if path:
            with open(path, mode="w", encoding="utf-8") as f:
                f.writelines(r_json)
        else:
            return r_json

    def export_py(self, path: str = None, sep='\n'):
        self.log.info(f"exporting note[{self.name}] to python file")
        lst_text = [
            re.sub(self._regex_interpreters, r"#\g<1>", p["text"])
            for p in self.info["paragraphs"]]
        text = sep.join(lst_text)
        if path:
            with open(path, mode="w", encoding="utf-8") as f:
                f.writelines(text)
        else:
            return text

    def import_note(self, note: dict, verbose=False):
        if not isinstance(note, dict) \
            or "paragraphs" not in note \
            or "name" not in note:
            raise TypeError("wrong note format given, please make sure to use Note.build_note")

        self.log.info(f"importing note[{self.name}]")
        r_json = self._import_note(note)
        return Note(
            self.zeppelin,
            name=note["name"],
            note_id=r_json,
            verbose=verbose)

    def import_py(self,
        path: str,
        note_name: str,
        verbose=False,
        config=ZEPPELIN_PARAGRAPH_CONFIG,
        interpreter=ZEPPELIN_INTERPRETER,
        **open_kwargs):

        if "mode" in open_kwargs:
            del open_kwargs["mode"]

        with open(path, mode='r', **open_kwargs) as f:
            text = f.read()

        note = Note.build_note(
            note_name=note_name,
            text=text,
            config=config,
            interpreter=interpreter)
        return self.import_note(note=note, verbose=verbose)

    def create_paragraph(self,
            text: str,
            title=None,
            index: int = -1,
            config: dict = ZEPPELIN_PARAGRAPH_CONFIG,
            verbose: bool = False):

        self.log.info(f"creating paragraph in note[{self.name}]")
        r_json = self._create_paragraph(
            text=text,
            title=title,
            index=index,
            config=config)
        return Paragraph(self, paragraph_id=r_json, verbose=verbose)

    def get_paragraph_by_index(self, index: int, verbose=False):
        self.log.info(f"getting paragraph by index {index} from note[{self.name}]")
        paragraph = self.info["paragraphs"][index]
        return Paragraph(self, paragraph_id=paragraph["id"], verbose=verbose)

    def get_paragraph_by_id(self, id_: str, verbose=False):
        self.log.info(f"getting paragraph by id {id_} from note[{self.name}]")
        paragraphs = self.info["paragraphs"]
        for p in paragraphs:
            if id_ == p["id"]:
                return Paragraph(self, paragraph_id=p["id"], verbose=verbose)

        msg = f"unable to get paragraph {id_} from note[{self.name}]"
        self.log.warning(msg)
        raise IndexError(msg)

    def get_paragraph_by_pair(self, key: str, value, verbose=False):
        self.log.info(f"getting paragraph by key: {key} and value: {value} from note[{self.name}]")
        paragraphs = self.info["paragraphs"]
        for p in paragraphs:
            if value == p[key]:
                return Paragraph(self, paragraph_id=p["id"], verbose=verbose)

        msg = f"unable to get paragraph key: {key} and value: {value} from note[{self.name}]"
        self.log.warning(msg)
        raise IndexError(msg)

    def add_cron(self, cron: str, release_resource=False):
        self.log.info(f"adding cron '{cron}' to note[{self.name}]")
        return self._add_cron(cron=cron, release_resource=release_resource)

    def remove_cron(self):
        self.log.info(f"removing cron from note[{self.name}]")
        return self._remove_cron()

    def remove_cron(self):
        self.log.info(f"getting cron from note[{self.name}]")
        return self._get_cron()

    def get_permission(self):
        self.log.info(f"getting permission from note[{self.name}]")
        return self._get_permission()

    def set_permission(self,
            readers: list,
            owners: list,
            runners: list,
            writers: list):

        self.log.info(f"setting cron from note[{self.name}]")
        return self._set_permission(
            readers=readers,
            owners=owners,
            runners=runners,
            writers=writers)


class Paragraph(ParagraphBase):
    _regex_interpreter = re.compile(r"[\s\n]*(%[a-zA-Z0-9_\.]+)\s*\n+")

    def __init__(self,
            note: Note,
            paragraph_id: str,
            verbose: bool = False,
            info: dict = None):

        self.verbose = verbose
        self.log = logging.getLogger(__name__ + f".Paragraph")
        if verbose:
            logger.set_stream_log_level(self.log, verbose=verbose)

        super().__init__(note, paragraph_id)

        self._cache = info or self._get_info()
        re_grp = re.match(Paragraph._regex_interpreter, self._cache["text"])
        if re_grp:
            self.interpreter = re_grp.group(1)
        else:
            self.interpreter = ZEPPELIN_INTERPRETER

    @classmethod
    def build_paragraph(cls,
            text: str,
            title: str = None,
            config: dict = ZEPPELIN_PARAGRAPH_CONFIG,
            interpreter: str = ZEPPELIN_INTERPRETER):

        paragraph = {}
        if title:
            paragraph["title"] = title

        if config:
            paragraph["config"] = config

        if interpreter \
            and re.search(Paragraph._regex_interpreter, text) is None:
            text = f"%{interpreter}\n{text}"

        paragraph["text"] = text
        return paragraph

    @property
    def paragraph_id(self):
        return self._paragraph_id

    @property
    def text(self):
        self.log.debug(f"get paragraph '{self.paragraph_id}' text from cache")
        return re.sub(self._regex_interpreter, '', self._cache["text"])

    @text.setter
    def text(self, value):
        if not isinstance(value, str):
            raise TypeError("text value must be string")

        if self.interpreter and not re.match(self._regex_interpreter, value):
            value = f"%{self.interpreter}\n{value}"

        self.update(text=value)
        self._cache["text"] = value

    @property
    def title(self):
        self.log.debug(f"get paragraph '{self.paragraph_id}' title from cache")
        return self._cache["title"]

    @title.setter
    def title(self, value):
        if not isinstance(value, str):
            raise TypeError("title value must be string")

        self.update(title=value)
        self._cache["title"] = value

    @property
    def date_updated(self):
        self.log.debug(f"get paragraph '{self.paragraph_id}' date_updated from cache")
        return self._cache["dateUpdated"]

    @property
    def config(self):
        self.log.debug(f"get paragraph '{self.paragraph_id}' config from cache")
        return self._cache["config"]

    @config.setter
    def config(self, value):
        if not isinstance(value, str):
            raise TypeError("config value must be string")
        
        self.update(config=value)
        self._cache["config"] = value

    @property
    def settings(self):
        self.log.debug(f"get paragraph '{self.paragraph_id}' settings from cache")
        return self._cache["settings"]

    @property
    def job_name(self):
        self.log.debug(f"get paragraph '{self.paragraph_id}' jobName from cache")
        return self._cache["jobName"]

    @property
    def results(self):
        self.log.debug(f"get paragraph '{self.paragraph_id}' results from cache")
        return self._cache["results"]

    @property
    def date_created(self):
        self.log.debug(f"get paragraph '{self.paragraph_id}' dateCreated from cache")
        return self._cache["dateCreated"]

    @property
    def date_started(self):
        self.log.debug(f"get paragraph '{self.paragraph_id}' dateStarted from cache")
        return self._cache["dateStarted"]

    @property
    def date_finished(self):
        self.log.debug(f"get paragraph '{self.paragraph_id}' dateFinished from cache")
        return self._cache["dateFinished"]

    @property
    def status(self):
        self.log.debug(f"get paragraph '{self.paragraph_id}' status from cache")
        return self._cache["status"]
    
    @property
    def progress_update_intervals(self):
        self.log.debug(f"get paragraph '{self.paragraph_id}' progressUpdateIntervalMs from cache")
        return self._cache["progressUpdateIntervalMs"]

    def get_info(self):
        self.log.info(f"getting '{self.paragraph_id}' info")
        r_json = self._get_info()
        self._cache = r_json
        return r_json

    def get_status(self):
        self.log.info(f"getting '{self.paragraph_id}' status")
        r_json = self._get_status()
        return r_json
    
    def update_config(self, config: dict):
        self.log.info(f"updating '{self.paragraph_id}' config with {config}")
        return self._update_config(config)

    def update_text(self, text: str, title: str = None):
        self.log.info(f"updating '{self.paragraph_id}' text")
        return self._update_text(text, title=title)

    def update(self, **kwargs):
        if len(kwargs) == 0:
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

    def delete(self):
        return self._delete()

    def run(self, sync=True, option: dict = None):
        return self._run(sync=sync, option=option)

    def stop(self):
        return self._stop()

    def move_to_index(self, index: int):
        return self._move_to_index(index)