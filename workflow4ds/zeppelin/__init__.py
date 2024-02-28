import os
import re
import time
import logging

from .base import ZeppelinBase, NoteBase, ParagraphBase
from .. import logger
from ..settings import ZEPPELIN_INTERPRETER, ZEPPELIN_PARAGRAPH_CONFIG

__all__ = ["Zeppelin"]

# Zeppelin Module User-friendly

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
        """
        get an instance of note given either note name or note id
        note that it is allowed in Zeppelin that note names are duplicated
        in this case the returned note is the one with the oldest create time

        Parameters:
        note_name: str, default None
            prepend path if the note is located in directory
        note_id: str, default None
            id of note as you can find in the respective url
        """

        if note_name is None and note_id is None:
            raise ValueError("either name of note or node id should be given")

        self.log.info("getting note")
        if note_name is None:
            note_name = self._get_note_name_by_id(note_id)
        if note_id is None:
            note_id = self._get_note_id_by_name(note_name)
        
        return Note(self, name=note_name, note_id=note_id)

    def create_note(self, name: str, paragraphs: list = None):
        """
        create a note with name and optional paragraphs specified

        Parameters:
        name: str, name of new note, prepend directory path if needed
        paragraphs: list, default None, a list of paragraphs in dict type
        """

        self.log.info("creating note")
        r_json = self._create_note(name, paragraphs)
        assert isinstance(r_json, str)
        return Note(self, name, r_json)

    def delete_note(self, note_name: str = None, note_id: str = None):
        """
        delete a note from Zeppelin given either name or note id

        Parameters:
        note_name: str, default None
            prepend path if the note is located in directory
        note_id: str, default None
            id of note as you can find in the respective url
        """

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
        """
        import a note in dict type to Zeppelin
        this api might need further development

        Parameters:
        note: dict
            note information in dict type, refer to official doc for api detail
        verbose: bool, default False
            whether returned instance of new note prints info log
        """

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
        data: str,
        note_name: str,
        verbose=False,
        config=ZEPPELIN_PARAGRAPH_CONFIG,
        interpreter=ZEPPELIN_INTERPRETER,
        **open_kwargs):
        """
        import python file or python code string to Zeppelin as a new note

        Parameters:
        data: str
            python file path with suffix '.py', or raw string of python code
        note_name: str
            the corresponding note name, prepend note path before name if needed
        verbose: bool, default False
            whether returned instance of new note prints info log
        config: dict, default ZEPPELIN_PARAGRAPH_CONFIG
            paragragh configuration, see the official doc if you want to modify
        interpreter: dict, default ZEPPELIN_INTERPRETER
            name of interpreter, refer to settings.py to make change of default value
        """

        assert isinstance(data, str)

        if os.path.isfile(data) and data.endswith(".py"):
            if "mode" in open_kwargs:
                del open_kwargs["mode"]
            with open(data, mode='r', **open_kwargs) as f:
                data = f.read()

        d_note = Note.build_note(
            note_name=note_name,
            text=data,
            config=config,
            interpreter=interpreter)
        return self.import_note(note=d_note, verbose=verbose)

    def clone_note(self,
            new_note_name: str,
            note_name: str = None,
            note_id: str = None,
            verbose: bool = False):
        """
        make a copy of Zeppelin note with a new name

        Parameters:
        new_note_name: str
            name of new note, prepend note path before name if needed
        note_name: str
            note name to copy from, prepend note path if located in directory
        note_id: str, default None
            id of note as you can find in the respective url
        verbose: bool, default False
            whether returned instance of new note prints info log
        """

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
        return Note(self, name=new_note_name, note_id=r_json, verbose=verbose)

    def export_note(self,
            note_name: str = None,
            note_id: str = None,
            path: str = None):
        """
        export note from Zeppelin into json format given either note name or id
        return string of json by default, if path given, save to file instead

        Parameters:
        note_name: str
            note name to export, prepend note path if located in directory
        note_id: str, default None
            id of note as you can find in the respective url
        path: str, default None
            file path
        """

        if note_name is None and note_id is None:
            raise ValueError("either name of original note or node id should be given")
    
        if isinstance(path, str) and not os.path.exists(os.path.dirname(path)):
            raise NotADirectoryError("the specified path of file does not exist")

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
        """
        export note from Zeppelin into python format given either note name or id
        return string of json by default, if path given, save to file instead

        Parameters:
        note_name: str
            note name to export, prepend note path if located in directory
        note_id: str, default None
            id of note as you can find in the respective url
        path: str, default None
            file path
        sep: str, default '\n'
            the symbol to concat each paragraph context with
        """

        if note_name is None and note_id is None:
            raise ValueError("either name of original note or node id should be given")

        self.log.info(f"exporting note[{self.name}] to python file")
        note = self.get_note(note_name=note_name, note_id=note_id)

        return note.export_py(path=path, sep=sep)


class Note(NoteBase):
    _regex_py_sep = re.compile(r"(?s)(.*?)\n#*(%[\w\d_\.]+\s*\n+.*?)(?=\n\s*#+%[\w\d_\.]+|\Z)")

    def __init__(self,
                 zeppelin: Zeppelin,
                 name: str,
                 note_id: str,
                 verbose: bool = False):
        """
        Zeppelin Note API

        Parameters:
        zeppelin: Zeppelin
            Zeppelin instance, in order for many methods to work and interact with
        name: str, name of note
        note_id: str, note id as you can find in url
        """

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
        """
        build note from scratch
        use this to build correct note format along with Paragraph.build_paragraph

        Parameters:
        note_name: str
            note name to export, prepend note path if located in directory
        text: str, default None
            if provided, will parse and split it into paragraphs
            based on interpreter hints in text
        paragraphs: list, default None
            list of paragraphs of dict type built from Paragraph.build_paragraph
        config: dict, default ZEPPELIN_PARAGRAPH_CONFIG
            paragragh configuration, see the official doc if you want to modify
        interpreter: dict, default ZEPPELIN_INTERPRETER
            name of interpreter, refer to settings.py to make change of default value
        """

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
        else:
            lst_text = [t for tup_grp in lst_text for t in tup_grp]

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

    def run_all(self, sync=True):
        # run all codes in note's paragraphs
        self.log.info(f"running note[{self.name}]")
        if sync:
            res = self._run_all()
        else:
            res = [p.run(sync=False) for p in self.iter_paragraphs()]
        return res

    def stop_all(self):
        # stop all codes in note's paragraphs
        self.log.info(f"stoping note[{self.name}]")
        r_json = self._stop_all()
        return r_json

    def clear_all_result(self):
        # clear all results from note's paragraphs
        self.log.info(f"clearing all result in note[{self.name}]")
        r_json = self._clear_all_result()
        return r_json
    
    def get_all_status(self):
        # get all status from note's paragraphs
        self.log.info(f"getting all paragraph status in note[{self.name}]")
        r_json = self._get_all_status()
        return r_json

    def delete(self):
        # delete the entire note
        self.log.info(f"deleting note[{self.name}]")
        r_json = self._delete_note()
        return r_json

    def clone(self, name: str, verbose=False):
        """
        make a copy of current note with a new name

        Parameters:
        name: str, name of new note, prepend note path before name if needed
        verbose: bool, default False
            whether the returned instance of new note prints info log
        """

        self.log.info(f"cloning note[{self.name}]")
        r_json = self._clone_note(name=name)
        return Note(self.zeppelin,
            name=name,
            note_id=r_json,
            verbose=verbose)

    def export_note(self, path: str = None):
        """
        export the current note in json format
        return string of json by default, if path given, save to file instead

        Parameters:
        path: str, default None
            file path
        """

        self.log.info(f"exporting note[{self.name}]")
        r_json = self._export_note()
        if path:
            with open(path, mode="w", encoding="utf-8") as f:
                f.writelines(r_json)
        else:
            return r_json

    def export_py(self, path: str = None, sep='\n'):
        """
        export current note from Zeppelin into python format

        Parameters:
        path: str, default None
            file path
        sep: str, default '\n'
            the symbol to concat each paragraph context with

        Returns:
            string of json by default, if path given, save to file instead
        """

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
        """
        import a note in dict type to Zeppelin
        this api might need further development

        Parameters:
        note: dict
            note information in dict type, refer to official doc for api detail
        verbose: bool, default False
            whether returned instance of new note prints info log

        Returns:
            Note: the new Note instance parsed from note dict
        """

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
        """
        import python file or python code string to Zeppelin as a new note

        Parameters:
        data: str
            python file path with suffix '.py', or raw string of python code
        note_name: str
            the corresponding note name, prepend note path before name if needed
        verbose: bool, default False
            whether returned instance of new note prints info log
        config: dict, default ZEPPELIN_PARAGRAPH_CONFIG
            paragragh configuration, see the official doc if you want to modify
        interpreter: dict, default ZEPPELIN_INTERPRETER
            name of interpreter, refer to settings.py to make change of default value

        Returns:
            Note: the new Note instance parsed from py file/string
        """

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
            interpreter: str = ZEPPELIN_INTERPRETER,
            verbose: bool = False):
        """
        create a new paragraph in current note

        Parameters:
        text: str
            paragraph context to create with
        title: str, default None
            title of paragraph, leave it blank by default
        config: dict, default ZEPPELIN_PARAGRAPH_CONFIG
            paragragh configuration, see the official doc if you want to modify
        interpreter: dict, default ZEPPELIN_INTERPRETER
            name of interpreter, refer to settings.py to make change of default value
        verbose: bool, default False
        whether to print log on stdout, default to False

        Returns:
            Paragraph: the new paragraph instance
        """

        self.log.info(f"creating paragraph in note[{self.name}]")
        if interpreter \
            and re.search(Paragraph._regex_interpreter, text) is None:
            text = f"%{interpreter}\n{text}"

        r_json = self._create_paragraph(
            text=text,
            title=title,
            index=index,
            config=config)
        return Paragraph(self, paragraph_id=r_json, verbose=verbose)

    def get_all_paragraphs(self, verbose: bool = False,  wait_sec: int = 0.1):
        lst_paragraph = []
        for p in self.iter_paragraphs():
            paragraph = Paragraph(self, paragraph_id=p["id"], verbose=verbose)
            lst_paragraph.append(paragraph)
            time.sleep(wait_sec)

        return lst_paragraph

    def get_paragraph_by_index(self, index: int, verbose=False):
        """
        get specific paragraph by the order (index+1)

        Parameters:
        index: int
            paragraph index, starting from 0. 
        verbose: bool, default False
            whether to print log on stdout, default to False
        
        Returns:
            Paragraph: the paragraph instance according to index
        """

        self.log.info(f"getting paragraph by index {index} from note[{self.name}]")
        paragraph = self.info["paragraphs"][index]
        return Paragraph(self, paragraph_id=paragraph["id"], verbose=verbose)

    def get_paragraph_by_id(self, id_: str, verbose=False):
        """
        get specific paragraph by its id

        Parameters:
        id_: int
            paragraph id_, usually not visible for user, do not use if you don't know this
        verbose: bool, default False
            whether the returned paragraph prints log on stdout, default to False

        Returns:
            Paragraph: the paragraph instance according to id
        """

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

    def iter_paragraphs(self, verbose=False):
        for p in self.info.get("paragraphs", []):
            yield Paragraph(self, paragraph_id=p["id"], verbose=verbose)

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
            verbose: bool = False):

        """
        Zeppelin Note Paragraph API

        Parameters:
        note: Zeppelin note
            Note instance, in order for many methods to work and interact with
        name: str, name of note
        paragraph_id: str, paragraph id 
        """

        self.verbose = verbose
        self.log = logging.getLogger(__name__ + f".Paragraph")
        if verbose:
            logger.set_stream_log_level(self.log, verbose=verbose)

        super().__init__(note, paragraph_id)

        self._outdated = True
        self.__cache = None

    @classmethod
    def build_paragraph(cls,
            text: str,
            title: str = None,
            config: dict = ZEPPELIN_PARAGRAPH_CONFIG,
            interpreter: str = ZEPPELIN_INTERPRETER):
        """
        build paragraph from scratch
        use this to build correct paragraph format along with Note.build_note

        Parameters:
        text: str
            the context of paragraph, will automatically add interpreter hint
            if the context doesn't contain one
        title: str, default None
            title of 
        config: dict, default ZEPPELIN_PARAGRAPH_CONFIG
            paragragh configuration, see the official doc if you want to modify
        interpreter: dict, default ZEPPELIN_INTERPRETER
            name of interpreter, refer to settings.py to make change of default value
        """

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
    def _cache(self):
        if self._outdated:
            self.log.debug(f"get paragraph '{self.paragraph_id}' cache")
            self.__cache = self._get_info()

        self._outdated = False
        return self.__cache

    @property
    def interpreter(self):
        re_grp = re.match(Paragraph._regex_interpreter, self._cache["text"])
        return re_grp.group(1) if re_grp else ZEPPELIN_INTERPRETER

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
        self._outdated = True

    @property
    def title(self):
        self.log.debug(f"get paragraph '{self.paragraph_id}' title from cache")
        return self._cache["title"]

    @title.setter
    def title(self, value):
        if not isinstance(value, str):
            raise TypeError("title value must be string")

        self.update(title=value)
        self._outdated = True

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
        self._outdated = True

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
        self.__cache = r_json
        return r_json

    def get_status(self):
        self.log.info(f"getting '{self.paragraph_id}' status")
        r_json = self._get_status()
        return r_json
    
    def update_config(self, config: dict):
        self.log.info(f"updating '{self.paragraph_id}' config with {config}")
        r_json = self._update_config(config)
        self._outdated = True
        return r_json

    def update_text(self, text: str, title: str = None):
        self.log.info(f"updating '{self.paragraph_id}' text")
        r_json = self._update_text(text, title=title)
        self._outdated = True
        return r_json

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

        self._outdated = True

    def delete(self):
        self._outdated = True
        return self._delete()

    def run(self, sync=True, option: dict = None):
        self._outdated = True
        return self._run(sync=sync, option=option)

    def stop(self):
        self._outdated = True
        return self._stop()

    def move_to_index(self, index: int):
        self._outdated = True
        return self._move_to_index(index)