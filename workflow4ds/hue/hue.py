import copy
import gc
import csv
import json
from tqdm import tqdm
import logging
import os
import time
import traceback
import uuid
import getpass
from datetime import datetime
from html import unescape
from unicodedata import normalize
import requests

from .. import logger
from ..settings import HUE_BASE_URL, MAX_LEN_PRINT_SQL, HIVE_PERFORMANCE_SETTINGS, PROGRESSBAR, HUE_INACTIVE_TIME
from ..decorators import retry, ensure_login

__all__ = ["Notebook", "Beeswax"]


class Beeswax(requests.Session):
    def __init__(self,
                 username: str = None,
                 password: str = None,
                 base_url: str = None,
                 hive_settings=None,
                 verbose: bool = False):
        self.log = logging.getLogger(__name__ + f".Beeswax")
        if verbose:
            logger.set_stream_log_level(self.log, verbose=verbose)

        if base_url is None:
            self.base_url = HUE_BASE_URL
        else:
            self.base_url = base_url

        self.is_logged_in = False
        self.username = username
        self._password = password
        self.hive_settings = hive_settings
        self._set_hive(self.hive_settings)
        self.verbose = verbose

        super(Beeswax, self).__init__()

        self.headers["User-Agent"] = "Mozilla/5.0 (Windows NT 6.1; Win64; x64) " \
                                     "AppleWebKit/537.36 (KHTML, like Gecko) " \
                                     "Chrome/76.0.3809.100 Safari/537.36"
        if self.username is not None \
                and password is not None:
            self.login(self.username, password)

    def login(self, username: str = None, password: str = None):
        self.is_logged_in = False

        self.username = username or self.username
        self._password = password or self._password
        if self.username is None and self._password is None:
            raise ValueError("please provide username and password")

        if self.username is None and self._password is not None:
            raise KeyError("username must be specified with password")

        if self.username is not None and self._password is None:
            self._password = getpass.getpass("Please provide Hue password: ")

        self.log.debug(f"logging in for user: [{self.username}]")
        login_url = self.base_url + '/accounts/login/'
        self.get(login_url)
        self.headers["Referer"] = login_url

        form_data = dict(username=self.username,
                         password=self._password,
                         csrfmiddlewaretoken=self.cookies['csrftoken'],
                         next='/')

        res = self.post(login_url,
                        data=form_data,
                        cookies={},
                        headers=self.headers)

        if res.status_code != 200 \
                or f"var LOGGED_USERNAME = '';" in res.text:
            self.log.error('login failed for [%s] at %s'
                           % (self.username, self.base_url))
        else:
            self.log.info('login succeeful [%s] at %s'
                          % (self.username, self.base_url))

            self.is_logged_in = True
            self.headers["X-CSRFToken"] = self.cookies['csrftoken']
            self.headers["Content-Type"] = "application/x-www-form-urlencoded; " \
                                           "charset=UTF-8"

    def _set_hive(self, hive_settings):
        self.log.debug("setting up hive job")
        if hive_settings is not None and not isinstance(hive_settings, dict):
            raise TypeError("hive_settings should be None or instance of dict")

        if hive_settings is None:
            self.hive_settings = HIVE_PERFORMANCE_SETTINGS.copy()
        else:
            self.hive_settings = hive_settings

        if hasattr(self, "snippet"):
            self.snippet["properties"]["settings"] = \
                [{"key": k, "value": v} for k, v in self.hive_settings.items()]

    def execute(self, query, database='default', approx_time=5, attempt_times=100):
        self.log.debug(f"beeswax sending query: {query[: MAX_LEN_PRINT_SQL]}")
        query_data = {
            'query-query': query,
            'query-database': database,
            'settings-next_form_id': 0,
            'file_resources-next_form_id': 0,
            'functions-next_form_id': 0,
            'query-email_notify': False,
            'query-is_parameterized': True,
        }

        self.headers["Referer"] = self.base_url + '/beeswax'
        execute_url = self.base_url + '/beeswax/api/query/execute/'

        res = self.post(
            execute_url,
            data=query_data,
            headers=self.headers,
            cookies=self.cookies,
            )
        self.log.debug(f"beeswax response: {res.json()}")
        assert res.status_code == 200

        res_json = res.json()
        job_id = res_json['id']

        watch_url = self.base_url + res_json['watch_url']

        t_sec = int(approx_time)
        t_try = int(attempt_times)
        t_tol = t_sec * t_try

        for i in range(t_try):
            print('waiting %3d/%d secs for job %d: %s ...' %
                  (t_sec * i, t_tol, int(job_id), query[: MAX_LEN_PRINT_SQL]) + '\r', end='')
            r = self.post(
                watch_url,
                data=query_data,
                headers=self.headers,
                cookies=self.cookies
                )

            r_json = r.json()
            self.log.debug(f"beeswax watch job {int(job_id)} responds: {r_json}")
            try:

                if r_json['isSuccess']:
                    break
                else:
                    time.sleep(t_sec)
            except Exception as e:
                self.log.error(f"beeswax waiting job error with response: {r_json['message']}")
                self.log.exception(e)
                raise e

        return r_json

    def table_detail(self, table_name, database):
        self.log.debug(f"fetching beeswax table detail: {database}.{table_name}")
        url = self.base_url + '/beeswax/api/table/{database}/{table_name}?format=json' \
            .format(database=database, table_name=table_name)

        r = self.get(
            url,
            headers=self.headers,
            cookies=self.cookies,
            )
        self.log.debug(f"beeswax table_detail responses: {r.text}")
        r_json = r.json()

        return r_json


class Notebook(requests.Session):
    """
    Hue Notebook API
    An intergraded hiveql platform

    Parameters:
    username: str, default None
        Hue username, if not provided here, user need to call self.login manually
    password: str, Hue password, default None
        Hue password, if not provided here, user need to call self.login manually
    base_url: str, default None
        link to Hue server, default to BASE_URL
    name: str, default ""
        name of Hue notebook
    description: str, default ""
        description of Hue notebook
    hive_settings: dict, default PERFORMANT_SETTINGS in settings
        if you insist on hive default settings, set this parameter to {}
        if not provided, notebook would use PERFORMANT_SETTINGS
    verbose: bool, default False
        whether to print log on stdout, default False
    """

    def __init__(self,
                 username: str = None,
                 password: str = None,
                 name: str = "",
                 description: str = "",
                 base_url: str = None,
                 hive_settings=None,
                 verbose: bool = False):

        self.name = name
        self.description = description
        self.hive_settings = hive_settings
        self.verbose = verbose

        self.log = logging.getLogger(__name__ + f".Notebook[{name}]")
        if verbose:
            logger.set_stream_log_level(self.log, verbose=verbose)

        self._set_hive(self.hive_settings)
        if base_url is None:
            self.base_url = HUE_BASE_URL
        else:
            self.base_url = base_url

        self.username = username
        self._password = password

        super(Notebook, self).__init__()
        self.headers["Accept"] = "*/*"
        self.headers["User-Agent"] = "Mozilla/5.0 (Windows NT 6.1; Win64; x64) " \
                                     "AppleWebKit/537.36 (KHTML, like Gecko) " \
                                     "Chrome/76.0.3809.100 Safari/537.36"
        self.headers["Content-Type"] = "application/x-www-form-urlencoded; charset=UTF-8"

        if self.username is not None \
                and password is not None:
            self.login(self.username, password)

    @property
    def is_logged_in(self):
        if not hasattr(self, "_last_execute"):
            return False

        return time.perf_counter() - self._last_execute < HUE_INACTIVE_TIME

    def login(self, username: str = None, password: str = None):
        self.username = username or self.username
        self._password = password or self._password
        if self.username is None and self._password is None:
            raise ValueError("please provide username and password")

        if self.username is None and self._password is not None:
            raise KeyError("username must be specified with password")

        if self.username is not None and self._password is None:
            self._password = getpass.getpass("Please provide Hue password: ")

        if "X-Requested-With" in self.headers:
            del self.headers["X-Requested-With"]
        
        if "csrftoken" in self.cookies:
            del self.cookies["csrftoken"]
            if "X-CSRFToken" in self.headers:
                del self.headers["X-CSRFToken"]

        self.log.debug(f"logging in for user: [{self.username}]")
        res = self._login()
        if res.status_code != 302 \
                or "errorList" in res.text:
            self._password = None
            self.log.error('login failed for [%s] at %s'
                           % (self.username, self.base_url))
            raise ValueError('login failed for [%s] at %s'
                             % (self.username, self.base_url))
        else:
            self.log.info('login succeeful [%s] at %s'
                          % (self.username, self.base_url))

            self.headers["X-CSRFToken"] = self.cookies["csrftoken"]
            self.headers["X-Requested-With"] = "XMLHttpRequest"

            self._last_execute = time.perf_counter()
            self._prepare_notebook(self.name, self.description, self.hive_settings)

        return self

    @retry(__name__)
    def _login(self):
        login_url = self.base_url + '/accounts/login/'
        # get csrftoken and sessionid from login page
        self.get(login_url)
        self.headers["Origin"] = self.base_url
        self.headers["Referer"] = login_url
        self.headers["X-CSRFToken"] = self.cookies["csrftoken"]
        data = {
            "username": self.username,
            "password": self._password,
            "csrfmiddlewaretoken": self.cookies['csrftoken']
        }

        # if successfully logged in, hue backend will redirect webpage with http 302
        # but in our case we don't need it to spend time redirecting
        res = self.post(login_url, data=data, allow_redirects=False)
        return res

    def _create_notebook(self, name="", description=""):
        r_json = self.__create_notebook().json()
        self.notebook = r_json["notebook"]
        self.notebook["name"] = name
        self.notebook["description"] = description

    @ensure_login
    @retry(__name__)
    def __create_notebook(self):
        self.log.debug("creating notebook")
        url = self.base_url + "/notebook/api/create_notebook"
        res = self.post(
            url,
            data={
                "type": "hive",
                "directory_uuid": ""
                }
            )
        return res

    @ensure_login
    @retry(__name__)
    def _create_session(self):
        # remember that this api won't always init and return a new session
        # instead, it will return existing busy/idle session
        self.log.debug("creating session")
        url = self.base_url + "/notebook/api/create_session"

        payload = {
            "notebook": json.dumps({
                "id": None if "id" not in self.notebook else self.notebook["id"],
                "uuid": self.notebook["uuid"],
                "parentSavedQueryUuid": None,
                "isSaved": self.notebook["isSaved"],
                "sessions": self.notebook["sessions"],
                "type": self.notebook["type"],
                "name": self.notebook["name"],
                "description": self.notebook["description"],
            }),
            "session": json.dumps({"type": "hive"}),
        }

        res = self.post(url, data=payload)
        r_json = res.json()
        self.session = r_json["session"]
        return res

    def _set_hive(self, hive_settings):
        self.log.debug("setting up hive job")
        if hive_settings is not None and not isinstance(hive_settings, dict):
            raise TypeError("hive_settings should be None or instance of dict")

        if hive_settings is None:
            self.hive_settings = HIVE_PERFORMANCE_SETTINGS.copy()
        else:
            self.hive_settings = hive_settings

        if hasattr(self, "snippet"):
            self.snippet["properties"]["settings"] = \
                [{"key": k, "value": v} for k, v in self.hive_settings.items()]

    def _prepare_notebook(self,
                          name="",
                          description="",
                          hive_settings=None,
                          recreate_session=False):

        self.log.debug(f"preparing notebook[{name}]")
        self._create_notebook(name, description)

        if recreate_session:
            self.recreate_session(hive_settings)
        else:
            self._create_session()
            self._set_hive(hive_settings)

    def _prepare_snippet(self, sql: str = "", database="default"):
        self.log.debug("preparing snippet")
        statements_list = sql.split(";")
        timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S:%f")[:-3] + "Z"
        if hasattr(self, "snippet"):
            self.snippet["statement"] = sql
            self.snippet["statement_raw"] = sql
            self.snippet["statementsList"] = statements_list
            self.snippet["result"]["handle"]["has_more_statements"] = len(statements_list) > 1
            self.snippet["result"]["handle"]["statements_count"] = len(statements_list)
            self.snippet["result"]["statements_count"] = len(statements_list)
            self.snippet["result"]["startTime"] = timestamp
            self.snippet["result"]["endTime"] = timestamp
            self.snippet["database"] = database
            self.snippet["lastExecuted"] = int(datetime.now().timestamp() * 10 ** 3)
            self.snippet["status"] = "running"
        else:
            self.snippet = {
                "id": str(uuid.uuid4()),
                "type": "hive",
                "status": "running",
                "statementType": "text",
                "statement": sql,
                "statement_raw": sql,
                "statementsList": statements_list,
                "statementPath": "",
                "associatedDocumentUuid": None,
                "properties": {
                    "settings": [{"key": k, "value": v} for k, v in self.hive_settings.items()],
                    "files": [],
                    "functions": [],
                    "arguments": []},
                "result": {
                    "id": str(uuid.uuid4()),
                    "type": "table",
                    "handle": {
                        "has_more_statements": len(statements_list) > 1,
                        "statement_id": 0,
                        "statements_count": len(statements_list),
                        "previous_statement_hash": None
                        },
                    "statement_id": 0,
                    "statements_count": len(statements_list),
                    "fetchedOnce": False,
                    "startTime": timestamp,
                    "endTime": timestamp,
                    "executionTime": 0,
                    },
                "database": database,
                "lastExecuted": int(datetime.now().timestamp() * 10 ** 3),
                "wasBatchExecuted": False
                }

    @ensure_login
    def execute(self,
                sql: str,
                database: str = "default",
                print_log: bool = False,
                progressbar: bool = True,
                progressbar_offset: int = 0,
                sync=True):
        try:
            if hasattr(self, "snippet") and self.is_logged_in:
                self._close_statement()

            self._prepare_snippet(sql, database)
            self.notebook["snippets"] = [self.snippet]

            r_json = self._execute(sql).json()
            if r_json["status"] != 0:
                if "message" in r_json:
                    self.log.error(r_json["message"])
                    raise RuntimeError(r_json["message"])
                else:
                    self.log.error(r_json.text)
                    raise RuntimeError(r_json.text)

            self.notebook["id"] = r_json.get('history_id', self.notebook.get("id", None))
            self.notebook["uuid"] = r_json.get('history_uuid', self.notebook.get("uuid", None))
            self.notebook["isHistory"] = True
            self.notebook["isBatchable"] = True

            self.snippet["result"]["handle"] = r_json["handle"]
            self.snippet["status"] = "running"

            self._result = NotebookResult(self)
            if sync:
                self._result.await_result(print_log=print_log,
                                          progressbar=progressbar,
                                          progressbar_offset=progressbar_offset)

            return self._result
        except KeyboardInterrupt:
            if self.is_logged_in:
                self.cancel_statement()

            self.recreate_session()
            if self._result._progressbar:
                self._result._progressbar.close()
            raise KeyboardInterrupt

    @ensure_login
    @retry(__name__)
    def _execute(self, sql: str):
        sql_print = sql[: MAX_LEN_PRINT_SQL] + "..." \
            if len(sql) > MAX_LEN_PRINT_SQL \
            else sql
        self.log.info(f"executing sql: {sql_print}")
        url = self.base_url + "/notebook/api/execute/hive"
        res = self.post(url,
                        data={"notebook": json.dumps(self.notebook),
                              "snippet": json.dumps(self.snippet)},
                        )
        return res

    def set_priority(self, priority: str):
        """
        Set the priority for Hive Query

        :param priority: one of "VERY_HIGH", "HIGH", "NORMAL", "LOW", "VERY_LOW",
                         case insensitive
        """

        self.hive_settings["mapreduce.job.priority"] = priority.upper()
        self._set_hive(self.hive_settings)

    def set_backtick(self, as_regex):
        """
        Set usage of ` for Hive Query

        :param as_regex: boolean
        """

        if as_regex:
            self.hive_settings["spark.sql.parser.quotedRegexColumnNames"] = "true"
            self.hive_settings["hive.support.quoted.identifiers"] = "none"
        else:
            self.hive_settings["spark.sql.parser.quotedRegexColumnNames"] = "false"
            self.hive_settings["hive.support.quoted.identifiers"] = "column"

        self._set_hive(self.hive_settings)

    def set_engine(self, engine: str):
        """
        Set the execute engine for Hive Query

        :param engine: one of "mr", "tez", spark",
                       case insensitive
        """

        self.hive_settings["hive.execution.engine"] = engine.lower()
        if self.hive_settings["hive.execution.engine"] == 'mr':
            self.hive_settings["hive.input.format"] = "org.apache.hadoop.hive.ql.io.CombineHiveInputFormat"
        else:
            self.hive_settings["hive.input.format"] = "org.apache.hadoop.hive.ql.io.HiveInputFormat"

        self._set_hive(self.hive_settings)

    def set_memory_multiplier(self, multiplier: float):
        """
        Set the multiplier over default memory setup

        :param multiplier: e.g. if multiplier is 2. memory allocation would times 2
        """

        self.hive_settings["mapreduce.map.memory.mb"] = f"{2048. * multiplier:.0f}"
        self.hive_settings["mapreduce.reduce.memory.mb"] = f"{2048. * multiplier:.0f}"
        self.hive_settings["mapreduce.map.java.opts"] = \
            f"-Djava.net.preferIPv4Stack=true -Xmx{1700. * multiplier:.0f}m"
        self.hive_settings["mapreduce.reduce.java.opts"] = \
            f"-Djava.net.preferIPv4Stack=true -Xmx{1700. * multiplier:.0f}m"
        self.hive_settings["tez.runtime.io.sort.mb"] = f"{820. * multiplier:.0f}"
        self.hive_settings["hive.auto.convert.join.noconditionaltask.size"] = f"{209715200. * multiplier:.0f}"

        self._set_hive(self.hive_settings)

    def set_hive(self, key, val):
        self.hive_settings[key] = val
        self._set_hive(self.hive_settings)

    def unset_hive(self, key):
        if key not in self.hive_settings:
            self.log.warning(f"skipping unset_hive because '{key}' not manually set in hive settings")
        else:
            del self.hive_settings[key]
            self._set_hive(self.hive_settings)

    def recreate_session(self, hive_settings=None):
        if not hasattr(self, "session"):
            self._create_session()

        r_json = self._close_session().json()
        if r_json["status"] == 1:
            closed_session_id = ""
        else:
            closed_session_id = r_json["session"]["session"]["id"]

        r_json = self._create_session().json()
        new_session_id = r_json["session"]["id"]
        self.notebook["sessions"] = [self.session]
        self._set_hive(hive_settings)
        return closed_session_id, new_session_id

    @ensure_login
    @retry(__name__)
    def cancel_statement(self):
        self.log.info("cancelling statement")
        url = self.base_url + "/notebook/api/cancel_statement"
        res = self.post(url,
                        data={"notebook": json.dumps(self.notebook),
                              "snippet": json.dumps(self.snippet)},
                        )
        return res

    @ensure_login
    @retry(__name__)
    def _close_statement(self):
        self.log.debug(f"closing statement")
        url = self.base_url + "/notebook/api/close_statement"
        res = self.post(url,
                        data={"notebook": json.dumps(self.notebook),
                              "snippet": json.dumps(self.snippet)},
                        )
        return res

    @retry(__name__)
    @ensure_login
    def _close_session(self):
        self.log.debug(f"closing session")
        url = self.base_url + "/notebook/api/close_session/"
        res = self.post(url,
                        data={"session": json.dumps(self.session)}
                        )
        return res

    @ensure_login
    @retry(__name__)
    def close_notebook(self):
        if not hasattr(self, "notebook"):
            self.log.warning("notebook not created yet")
            return

        self.log.info(f"closing notebook")
        url = self.base_url + "/notebook/api/notebook/close/"
        res = self.post(url,
                        data={"notebook": json.dumps(self.notebook)}
                        )
        return res

    def logout(self):
        self._last_execute = 0.
        return self._logout()

    @retry(__name__)
    def _logout(self):
        self.log.info(f"logging out")

        url = self.base_url + "/accounts/logout/"
        res = self.get(url, allow_redirects=False)
        return res

    def new_notebook(self,
                     name="", description="",
                     hive_settings=None,
                     recreate_session=False,
                     verbose: bool = None):
        new_nb = copy.deepcopy(self)

        new_nb.username = self.username
        new_nb.name = name
        new_nb.description = description
        new_nb.base_url = self.base_url
        new_nb.hive_settings = hive_settings
        new_nb.username = self.username
        new_nb._password = self._password
        new_nb._last_execute = self._last_execute
        new_nb.verbose = self.verbose if verbose is None else verbose

        new_nb.log = logging.getLogger(__name__ + f".Notebook[{name}]")
        if new_nb.verbose:
            logger.set_stream_log_level(new_nb.log, verbose=new_nb.verbose)

        if recreate_session:
            new_nb._prepare_notebook(name, description,
                                     hive_settings=hive_settings,
                                     recreate_session=True)
        else:
            new_nb._create_notebook(name, description)
            new_nb.notebook["sessions"] = [self.session]
            new_nb.session = self.session
            new_nb._set_hive(hive_settings)

        return new_nb

    @ensure_login
    @retry(__name__)
    def _clear_history(self):
        self.log.info(f"clearing history")
        url = self.base_url + f'/notebook/api/clear_history/'
        res = self.post(url,
                        data={
                            "notebook": json.dumps(self.notebook),
                            "doc_type": "hive"
                        })
        return res

    def clear_history(self, simple=False):
        self._clear_history()
        if not simple:
            self._prepare_notebook(self.name, self.description)

    def close(self):
        if self.is_logged_in:
            if hasattr(self, "snippet"):
                self._close_statement()

            if hasattr(self, "notebook"):
                self.close_notebook()

        super(Notebook, self).close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, tb):
        if exc_type is not None:
            traceback.print_exception(exc_type, exc_value, tb)

        self.close()
        self.logout()


class NotebookResult(object):
    """
    An integrated class to interact with executed sql result
    """

    def __init__(self, notebook):
        self.name = notebook.name
        self.base_url = notebook.base_url
        self.notebook = copy.deepcopy(notebook.notebook)
        self.snippet = copy.deepcopy(notebook.snippet)
        self.is_logged_in = notebook.is_logged_in
        self.verbose = notebook.verbose

        self.log = logging.getLogger(__name__ + f".NotebookResult[{self.name}]")
        if self.verbose:
            logger.set_stream_log_level(self.log, verbose=self.verbose)

        self._progressbar_format = PROGRESSBAR.copy()
        self._progressbar_format["desc"] = PROGRESSBAR["desc"].format(name=self.name, result="result")
        self.data = None
        self.full_log = ""
        self._last_check = None
        self._logs_row = 0
        self._app_ids = set()
        self._app_id = ''
        self._progress = 0.
        self._progressbar = None
        self._progressor = self._progress_updater()

        self._notebook = notebook
        # the proxy might fail to respond when the response body becomes too large
        # manually set it smaller if so
        self.rows_per_fetch = 32768

    def is_ready(self):
        return self.snippet["status"] == "available"

    @retry(__name__)
    def _check_status(self):
        url = self.base_url + "/notebook/api/check_status"
        payload = {
            "notebook": json.dumps({
                "id": None if "id" not in self.notebook else self.notebook["id"],
                "uuid": self.notebook["uuid"],
                "parentSavedQueryUuid": None,
                "isSaved": self.notebook["isSaved"],
                "sessions": self.notebook["sessions"],
                "type": self.notebook["type"],
                "name": self.notebook["name"],
            }),
            "snippet": json.dumps(self.snippet)
        }
        res = self._notebook.post(url, data=payload)
        return res

    def check_status(self, return_log=False, update_interval=60.):
        self.log.info(f"checking {'yarn app: ' + self._app_id if len(self._app_id) else 'status'}")
        if len(self._app_id) > 0:
            r_json = self._get_app_info(self._app_id).json()
            if 'message' in r_json:
                self.log.warning(f"cannot find {self._app_id}")
                return self.snippet["status"]

            r_json = r_json["job"]
            progress = r_json["progress"]
            # value of progress might become "", indicating the query is available or failed
            # at this point the progress will just keep its original value
            # afterward let _check_status update the true status of query
            self._progress = float(progress) if progress else self._progress

        # init time counter
        cur_check = time.perf_counter()
        if self._last_check is None:
            self._last_check = cur_check
            will_update_status = True
        else:
            will_update_status = cur_check - self._last_check > update_interval

        # fetch cloud log by default
        try:
            cloud_log = self.fetch_cloud_logs()
        except RuntimeError:
            cloud_log = ''
        # call _check_status api only when the result has final status
        if "ERROR  :" in cloud_log:
            will_update_status = True
            self.log.error(cloud_log)
        elif "INFO  : OK" in cloud_log:
            will_update_status = True

        if will_update_status:
            self._last_check = cur_check
            # session won't quit if this api is not called, causing "Too many opened session" error
            # _check_status responses slow, do not call it while it's not necessary
            r_json = self._check_status().json()
            if r_json["status"] != 0:
                if len(cloud_log) > 0:
                    self.log.error(cloud_log)

                if "message" in r_json:
                    raise RuntimeError(r_json["message"])
                else:
                    raise RuntimeError(r_json)

            status = r_json["query_status"]["status"]
            self.snippet["status"] = status

        if return_log:
            return cloud_log
        return self.snippet["status"]

    def await_result(self, wait_sec: int = 1, print_log=False, progressbar=True, progressbar_offset=0):
        start_time = time.perf_counter()
        while print_log:
            time.sleep(wait_sec)
            self.log.debug(f"awaiting result elapsed {time.perf_counter() - start_time:.2f} secs")
            cloud_log = self.check_status(return_log=print_log)
            if len(cloud_log) > 0:
                print(cloud_log)

            if self.is_ready():
                self.log.debug(f"sql execution done in {time.perf_counter() - start_time:.2f} secs")
                return

        if progressbar:
            self._progressbar = tqdm(total=100, position=progressbar_offset, **self._progressbar_format)

        while True:
            time.sleep(wait_sec)
            self.check_status()
            if progressbar:
                self.update_progressbar(self._progressbar)
            if self.is_ready():
                self.log.debug(f"sql execution done in {time.perf_counter() - start_time:.2f} secs")
                if progressbar:
                    self._progressbar.close()
                return

    @retry(__name__)
    def _fetch_result(self, rows: int = None, start_over=False):
        self.log.debug(f"fetching result")
        url = self.base_url + f'/notebook/api/fetch_result_data/'
        payload = {
            "notebook": json.dumps(self.notebook),
            "snippet": json.dumps(self.snippet),
            "rows": rows if isinstance(rows, int) else self.rows_per_fetch,
            "startOver": "true" if start_over else "false"
            }

        res = self._notebook.post(url, data=payload, stream=True)
        return res

    def fetchall(self, progressbar=True, total=None, progressbar_offset=0):
        self.log.info(f"fetching all")
        if not self.is_ready():
            self.log.warning(f"result {self.snippet['status']}")

        if total is None:
            total, size = self.fetch_result_size()

        if progressbar:
            setup_progressbar = PROGRESSBAR.copy()
            setup_progressbar["desc"] = setup_progressbar["desc"].format(
                name=self.name,
                result='fetchall')
            if not isinstance(total, int):
                setup_progressbar["bar_format"] = '{l_bar}{n_fmt}{unit}, {rate_fmt}{postfix} |{elapsed}'
            else:
                setup_progressbar["bar_format"] = '{l_bar}{bar:25}|[{elapsed}<{remaining}]'
            pbar = tqdm(total=total,
                        position=progressbar_offset,
                        unit="rows",
                        **setup_progressbar)

        res = self._fetch_result(start_over=True)
        res = res.json()["result"]

        lst_data = [[s if not isinstance(s, str)
                     else '' if s == "NULL"
                     else normalize("NFKC", unescape(s))
                     for s in row]
                    for row in res["data"]]

        lst_metadata = [m["name"].rpartition(".")[2]
                        for m in res["meta"]]

        if progressbar:
            pbar.update(len(res["data"]))

        while res["has_more"]:
            res = self._fetch_result(start_over=False)
            try:
                res = res.json()["result"]
            except MemoryError:
                gc.collect()
                res = res.json()["result"]
            finally:
                lst_data.extend([[s if not isinstance(s, str)
                                  else '' if s == "NULL"
                                  else normalize("NFKC", unescape(s))
                                  for s in row]
                                 for row in res["data"]])
                if progressbar:
                    pbar.update(len(res["data"]))

        if progressbar:
            pbar.close()
        self.data = {"data": lst_data, "columns": lst_metadata}
        return self.data

    @retry(__name__)
    def _fetch_result_size(self):
        url = self.base_url + "/notebook/api/fetch_result_size"
        payload = {
            "notebook": json.dumps({
                "id": None if "id" not in self.notebook else self.notebook["id"],
                "uuid": self.notebook["uuid"],
                "parentSavedQueryUuid": None,
                "isSaved": self.notebook["isSaved"],
                "sessions": self.notebook["sessions"],
                "type": self.notebook["type"],
                "name": self.notebook["name"],
            }),
            "snippet": json.dumps(self.snippet)
        }
        res = self._notebook.post(url, data=payload)
        return res

    def fetch_result_size(self):
        # this might be inaccurate when sql contains `limit` clause
        res = self._fetch_result_size()
        res = res.json()
        if res["status"] != 0:
            if "result" not in res:
                self.log.warning(res)
                return None, None
        
            res = res["result"]
            if "message" in res:
                self.log.warning(res["message"])
            else:
                self.log.warning(res)
            return None, None

        res = res["result"]
        if "message" in res and isinstance(res["message"], str):
            self.log.warning(res["message"])

        return res["rows"], res["size"]

    def fetch_cloud_logs(self):
        self.log.debug("fetching cloud logs")
        res = self._get_logs(self._logs_row, self.full_log)
        cloud_log = res.json()
        if "logs" not in cloud_log:
            if "message" in cloud_log:
                self.log.warning(f"fetching_cloud_logs responses: {cloud_log['message']}")
            else:
                self.log.error(f"Could not parse logs from cloud response: {res.text}")
                raise RuntimeError(f"Could not parse logs from cloud response: {res.text}")

        for i, job in enumerate(cloud_log["jobs"]):
            if job["started"] and not job["finished"]:
                self._app_id = job["name"]

            self._app_ids.add(job["name"])

        progress = cloud_log["progress"]
        cloud_log = cloud_log["logs"]

        self._progress = self._progress if self._progress > progress else progress
        if len(cloud_log) > 0:
            self.full_log += "\n" + cloud_log if len(self.full_log) > 0 else cloud_log
            self._logs_row += 1 + cloud_log.count("\n")

        return cloud_log

    def update_progressbar(self, pbar, desc=None):
        if desc is None:
            desc = PROGRESSBAR["desc"].format(
                name=self.name,
                result=self._app_id if self._app_id else 'result')
        pbar.set_description(desc)
        pbar.update(next(self._progressor))

    def _progress_updater(self):
        last_progress, progress = 0, 0
        while not self.is_ready():
            last_progress, progress = progress, self._progress
            yield progress - last_progress

        self._progress = 100.
        yield self._progress - progress

    @property
    def app_id(self):
        if len(self._app_ids) == 0:
            self.fetch_cloud_logs()

        return self._app_id

    @retry(__name__)
    def _get_app_info(self, app_id):
        url = HUE_BASE_URL + f"/jobbrowser/jobs/{app_id}"
        res = self._notebook.get(url,
                                 params={"format": "json"}, )
        return res

    @retry(__name__)
    def _get_logs(self, start_row, full_log):
        url = self.base_url + "/notebook/api/get_logs"
        payload = {
            "notebook": json.dumps({
                "id": None if "id" not in self.notebook else self.notebook["id"],
                "uuid": self.notebook["uuid"],
                "parentSavedQueryUuid": None,
                "isSaved": self.notebook["isSaved"],
                "sessions": self.notebook["sessions"],
                "type": self.notebook["type"],
                "name": self.notebook["name"],
            }),
            "snippet": json.dumps(self.snippet),
            "from": start_row,
            "jobs": [],  # api won't read jobs, so pass an empty one won't do harm to anything
            "full_log": full_log
        }

        res = self._notebook.post(url, data=payload)
        return res

    def to_csv(self,
               file_name: str = None,
               encoding="utf-8",
               column_names: list = None,
               total: int = None,
               progressbar=True,
               progressbar_offset=0):
        """
        Download result of executed sql directly into a csv file.
        For now, only support csv file.

        :param file_name:  default notebook name
        :param encoding: file encoding, default to utf-8
        :param column_names: column names to rename to, default to original names
        :param total: a hint of rows passed by user,
                      if None passed and show progressbar, will try fetch_result_size api
        :param progressbar: default to True, whether to show progressbar
        :param encoding: file encoding, default to utf-8
        """
        if file_name is None:
            file_name = os.path.join(os.getcwd(), self.name + ".csv")

        if file_name.rpartition(".")[2] != "csv":
            file_name += ".csv"

        abs_dir = os.path.abspath(os.path.dirname(file_name))
        base_name = os.path.basename(file_name)
        if not os.path.exists(abs_dir):
            os.makedirs(abs_dir)

        abs_path = os.path.join(abs_dir, base_name)

        self.log.info(f"downloading to {abs_path}")
        if progressbar:
            if total is None:
                total, size = self.fetch_result_size()

            setup_progressbar = PROGRESSBAR.copy()
            setup_progressbar["desc"] = setup_progressbar["desc"].format(
                name=self.name,
                result='fetchall')
            if not isinstance(total, int):
                setup_progressbar["bar_format"] = '{l_bar}{n_fmt}{unit}, {rate_fmt}{postfix} |{elapsed}'
            else:
                setup_progressbar["bar_format"] = '{l_bar}{bar:25}|[{elapsed}<{remaining}]'
            pbar = tqdm(total=total,
                        position=progressbar_offset,
                        unit="rows",
                        **setup_progressbar)

        with open(abs_path, "w", newline="", encoding=encoding) as f:
            writer = csv.writer(f)

            res = self._fetch_result(start_over=True)
            res = res.json()["result"]
            lst_data = [[s if not isinstance(s, str)
                         else '' if s == "NULL"
                         else normalize("NFKC", unescape(s))
                         for s in row]
                        for row in res["data"]]

            lst_metadata = [m["name"].rpartition(".")[2]
                            for m in res["meta"]]

            if column_names:
                writer.writerow(column_names)
            else:
                writer.writerow(lst_metadata)

            writer.writerows(lst_data)

            if progressbar:
                pbar.update(len(res["data"]))

            while res["has_more"]:
                res = self._fetch_result(start_over=False)
                res = res.json()["result"]
                lst_data = [[s if not isinstance(s, str)
                             else '' if s == "NULL"
                             else normalize("NFKC", unescape(s))
                             for s in row]
                            for row in res["data"]]

                writer.writerows(lst_data)
                if progressbar:
                    pbar.update(len(res["data"]))

        if progressbar:
            pbar.close()
