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
from datetime import datetime
from html import unescape
from unicodedata import normalize
import requests

from . import logger
from .settings import HUE_BASE_URL, MAX_LEN_PRINT_SQL, HIVE_PERFORMANCE_SETTINGS
from .decorators import retry, ensure_login

__all__ = ["Notebook", "Beeswax"]


class Beeswax(requests.Session):
    def __init__(self,
                 username: str = None,
                 password: str = None,
                 base_url: str = None,
                 hive_settings=HIVE_PERFORMANCE_SETTINGS,
                 verbose: bool = False):

        self.hive_settings = hive_settings
        self.verbose = verbose

        self.log = logging.getLogger(__name__ + f".Beeswax")
        logger.set_log_level(self.log, verbose=verbose)

        if base_url is None:
            self.base_url = HUE_BASE_URL
        else:
            self.base_url = base_url

        self.is_logged_in = False
        self.username = username
        self._password = password

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
            print("Please provide Hue password:", end='')
            self._password = input("")

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
            self.log.exception('login failed for [%s] at %s'
                               % (self.username, self.base_url))
        else:
            self.log.info('login succeeful [%s] at %s'
                          % (self.username, self.base_url))

            self.is_logged_in = True
            self.headers["X-CSRFToken"] = self.cookies['csrftoken']
            self.headers["Content-Type"] = "application/x-www-form-urlencoded; " \
                                           "charset=UTF-8"

    def execute(self, query, database='buffer_fk', approx_time=5, attempt_times=100):
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

    Parameters：
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
        logger.set_log_level(self.log, verbose=verbose)

        if base_url is None:
            self.base_url = HUE_BASE_URL
        else:
            self.base_url = base_url

        self.is_logged_in = False
        self.username = username
        self._password = password

        super(Notebook, self).__init__()
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
            print("Please provide Hue password:", end='')
            self._password = input("")

        self.log.debug(f"logging in for user: [{self.username}]")
        res = self._login()
        if res.status_code != 200 \
                or f"var LOGGED_USERNAME = '';" in res.text:
            self.log.exception('login failed for [%s] at %s'
                               % (self.username, self.base_url))
            self._password = None
        else:
            self.log.info('login succeeful [%s] at %s'
                          % (self.username, self.base_url))

            self.is_logged_in = True
            self.headers["X-CSRFToken"] = self.cookies['csrftoken']
            self.headers["Content-Type"] = "application/x-www-form-urlencoded; " \
                                           "charset=UTF-8"
            self.headers["X-Requested-With"] = "XMLHttpRequest"

            self._prepare_notebook(self.name, self.description, self.hive_settings)

        return self

    @retry(__name__)
    def _login(self):
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
        return res

    def _create_notebook(self, name="", description=""):
        r_json = self.__create_notebook().json()
        self.notebook = r_json["notebook"]
        self.notebook["name"] = name
        self.notebook["description"] = description

    @retry(__name__)
    @ensure_login
    def __create_notebook(self):
        self.log.debug("creating notebook")
        url = self.base_url + "/notebook/api/create_notebook"
        self.headers["Host"] = "10.19.185.29:8889"
        self.headers["Referer"] = "http://10.19.185.29:8889/hue/editor/?type=hive"

        res = self.post(
            url,
            headers=self.headers,
            cookies=self.cookies,
            data={
                "type": "hive",
                "directory_uuid": ""
                }
            )
        self.log.debug(f"create notebook response: {res.text}")
        return res

    @retry(__name__)
    @ensure_login
    def _create_session(self):
        # remember that this api won't always init and return a new session
        # instead, it will return existing busy/idle session
        self.log.debug("creating session")
        url = self.base_url + "/notebook/api/create_session"
        self.headers["Host"] = "10.19.185.29:8889"
        self.headers["Referer"] = "http://10.19.185.29:8889/hue/editor/?type=hive"

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

        r = self.post(
            url,
            headers=self.headers,
            cookies=self.cookies,
            data=payload
        )
        self.log.debug(f"create session response: {r.text}")
        r_json = r.json()
        self.session = r_json["session"]
        return r

    def _set_hive(self, hive_settings):
        self.log.debug("setting up hive job")
        if hive_settings is not None and not isinstance(hive_settings, dict):
            raise TypeError("hive_settings should be None or instance of dict")

        if hive_settings is None:
            self.hive_settings = HIVE_PERFORMANCE_SETTINGS
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
                sync=True):
        try:
            if hasattr(self, "snippet"):
                self._close_statement()

            self._prepare_snippet(sql, database)
            self.notebook["snippets"] = [self.snippet]

            r_json = self._execute(sql).json()
            if r_json["status"] != 0:
                self.log.exception(r_json["message"])
                raise RuntimeError(r_json["message"])

            self.notebook["id"] = r_json["history_id"]
            self.notebook["uuid"] = r_json["history_uuid"]
            self.notebook["isHistory"] = True
            self.notebook["isBatchable"] = True

            self.snippet["result"]["handle"] = r_json["handle"]
            self.snippet["status"] = "running"

            self._result = NotebookResult(self)
            if sync:
                self._result.await_result(print_log=print_log)

            return self._result
        except KeyboardInterrupt:
            self.cancel_statement()
            self.recreate_session()
            raise KeyboardInterrupt

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
        self.log.debug(f"_execute returns: {res.text}")
        return res

    def set_priority(self, priority: str):
        """
        Set the priority for Hive Query

        :param priority: one of "VERY_HIGH", "HIGH", "NORMAL", "LOW", "VERY_LOW",
            case insensitive
        """

        self.hive_settings["mapreduce.job.priority"] = priority.upper()
        self._set_hive(self.hive_settings)

    def set_engine(self, engine: str):
        """
        Set the priority for Hive Query

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
        self.log.debug(f"cancel statement response: {res.text}")
        return res

    @retry(__name__)
    @ensure_login
    def _close_statement(self):
        self.log.debug(f"closing statement")
        url = self.base_url + "/notebook/api/close_statement"
        res = self.post(url,
                        data={"notebook": json.dumps(self.notebook),
                              "snippet": json.dumps(self.snippet)},
                        )
        self.log.debug(f"close statement response: {res.text}")
        return res

    @retry(__name__)
    @ensure_login
    def _close_session(self):
        self.log.debug(f"closing session")
        url = self.base_url + "/notebook/api/close_session/"
        res = self.post(url,
                        data={"session": json.dumps(self.session)}
                        )
        self.log.debug(f"close session response: {res.text}")
        return res

    @retry(__name__)
    @ensure_login
    def close_notebook(self):
        if not hasattr(self, "notebook"):
            self.log.warning("notebook not created yet")
            return

        self.log.info(f"closing notebook")
        url = self.base_url + "/notebook/api/notebook/close/"
        res = self.post(url,
                        data={"notebook": json.dumps(self.notebook)}
                        )
        self.log.debug(f"close notebook response: {res.text}")
        return res

    def logout(self):
        self.is_logged_in = False
        return self._logout()

    @retry(__name__)
    def _logout(self):
        self.log.info(f"logging out")

        url = self.base_url + "/accounts/logout/"
        res = self.get(url)
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
        new_nb.is_logged_in = self.is_logged_in
        new_nb.verbose = self.verbose if verbose is None else verbose

        new_nb.log = logging.getLogger(__name__ + f".Notebook[{name}]")
        logger.set_log_level(new_nb.log)

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

    @retry(__name__)
    @ensure_login
    def _clear_history(self):
        self.log.info(f"clearing history")
        url = self.base_url + f'/notebook/api/clear_history/'
        res = self.post(url,
                        data={
                            "notebook": json.dumps(self.notebook),
                            "doc_type": "hive"
                            })
        self.log.debug(f"clear history response: {res.text}")
        return res

    def clear_history(self, simple=False):
        self._clear_history()
        if not simple:
            self._prepare_notebook(self.name, self.description)

    def close(self):
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
        logger.set_log_level(self.log, verbose=self.verbose)

        self.data = None
        self.full_log = ""
        self._logs_row = 0
        self._app_ids = set()
        self._app_id = ''
        self._progress = 0.

        self._notebook = notebook
        # the proxy might fail to respond when the response body becomes too large
        # manually set it smaller if so
        self.rows_per_fetch = 32768

    @retry(__name__)
    def _check_status(self):
        url = self.base_url + "/notebook/api/check_status"
        res = self._notebook.post(url,
                                  data={"notebook": json.dumps({"id": self.notebook["uuid"]})}
                                  )
        self.log.debug(f"_check status response: {res.text}")
        return res

    def check_status(self, return_log=False):
        app_id = self.app_id
        self.log.debug(f"checking {'yarn app: ' + ', '.join(app_id) if len(app_id) else 'status'}")

        res = self._check_status()
        r_json = res.json()

        # fetch cloud log by default
        cloud_log = self.fetch_cloud_logs()
        if r_json["status"] != 0:
            if len(cloud_log) > 0:
                self.log.exception(cloud_log)

            if "message" in r_json:
                raise RuntimeError(r_json["message"])
            else:
                raise RuntimeError(r_json)

        status = r_json["query_status"]["status"]
        self.snippet["status"] = status
        if status != "running" and status != "available":
            self.log.exception(f"query {status}")
            raise RuntimeError(f"query {status}")

        if return_log:
            return cloud_log

        return status

    def await_result(self, wait_sec: int = 1, print_log=False):
        start_time = time.perf_counter()
        while print_log:
            time.sleep(wait_sec)
            self.log.debug(f"awaiting result "
                           f"elapsed {time.perf_counter() - start_time:.2f} secs")
            cloud_log = self.check_status(return_log=print_log)
            if len(cloud_log) > 0:
                print(cloud_log)

            if self.snippet["status"] == "available":
                self.log.debug(f"sql execution done in {time.perf_counter() - start_time:.2f} secs")
                return

        pbar = tqdm(total=100, bar_format='{l_bar}{bar}|{elapsed}', desc="awaiting result")
        while True:
            time.sleep(wait_sec)
            self.check_status()
            if len(self._app_id) > 0:
                pbar.set_description(f"awaiting {self._app_id}")
                pbar.update(self.update_progress(self._app_id))
            else:
                pbar.set_description(f"awaiting result")
                pbar.update(0.)

            if self.snippet["status"] == "available":
                self.log.debug(f"sql execution done in {time.perf_counter() - start_time:.2f} secs")
                pbar.set_description(f"awaiting {self._app_id if self._app_id else 'result'}")
                pbar.update(100. - self._progress)
                self._progress = 100.
                pbar.close()
                return

    def is_ready(self):
        self.check_status()
        return self.snippet["status"] == "available"

    @retry(__name__)
    def _fetch_result(self, rows: int = None, start_over=False):
        self.log.debug(f"fetching result")
        if not self.snippet["status"] == "available":
            raise AssertionError(f"result {self.snippet['status']}")

        url = self.base_url + f'/notebook/api/fetch_result_data/'
        payload = {
            "notebook": json.dumps(self.notebook),
            "snippet": json.dumps(self.snippet),
            "rows": rows if isinstance(rows, int) else self.rows_per_fetch,
            "startOver": "true" if start_over else "false"
            }

        res = self._notebook.post(url, data=payload)
        return res

    def fetchall(self):
        self.log.info(f"fetching all")
        res = self._fetch_result(start_over=True)
        res = res.json()["result"]

        lst_data = [[normalize("NFKC", unescape(s))
                     if isinstance(s, str) else s
                     for s in row]
                    for row in res["data"]]

        lst_metadata = [m["name"].rpartition(".")[2]
                        for m in res["meta"]]

        while res["has_more"]:
            res = self._fetch_result(start_over=False)
            try:
                res = res.json()["result"]
            except MemoryError:
                gc.collect()
                res = res.json()["result"]
            finally:
                lst_data.extend([[normalize("NFKC", unescape(s))
                                  if isinstance(s, str) else s
                                  for s in row]
                                 for row in res["data"]])

        self.data = {"data": lst_data, "columns": lst_metadata}
        return self.data

    def fetch_cloud_logs(self):
        self.log.debug("fetching cloud logs")
        res = self._get_logs(self._logs_row, self.full_log)
        cloud_log = res.json()
        if "logs" not in cloud_log:
            if "message" in cloud_log:
                self.log.warning(f"fetching_cloud_logs responses: {cloud_log['message']}")
            else:
                self.log.warning(f"Could not parse logs from cloud response: {res.text}")
            return ''

        for i, job in enumerate(cloud_log["jobs"]):
            if job["started"] and not job["finished"]:
                self._app_id = job["name"]

            self._app_ids.add(job["name"])

        cloud_log = cloud_log["logs"]
        if len(cloud_log) > 0:
            self.full_log += "\n" + cloud_log if len(self.full_log) > 0 else cloud_log
            self._logs_row += 1 + cloud_log.count("\n")

        return cloud_log

    def update_progress(self, app_id: str):
        res = self._get_app_info(app_id).json()
        if 'message' in res:
            self.log.warning(res["message"])
            return 0.
        elif 'uri' in res:
            self.log.warning(f"cannot read {app_id} progress")
            return 0.

        progress = res["job"]["progress"]
        if isinstance(progress, (float, int)):
            self._progress, progress = progress, self._progress
        elif isinstance(progress, str) and len(progress) == 0:
            progress = self._progress

        return self._progress - progress

    @property
    def app_id(self):
        if len(self._app_ids) == 0:
            self.fetch_cloud_logs()

        return self._app_id

    @retry(__name__)
    def _get_app_info(self, app_id):
        url = self.base_url + f'/jobbrowser/jobs/{app_id}?format=json'
        res = self._notebook.post(url)
        return res

    @retry(__name__)
    def _get_logs(self, start_row, full_log):
        url = self.base_url + "/notebook/api/get_logs"
        payload = {
            "notebook": json.dumps(self.notebook),
            "snippet": json.dumps(self.snippet),
            "from": start_row,
            "jobs": [],  # api won't read jobs, so pass an empty one won't do harm to anything
            "full_log": full_log
        }

        res = self._notebook.post(url, data=payload)
        return res

    def to_csv(self, file_name: str = None, encoding="utf-8"):
        """
        Download result of executed sql directly into a csv file.
        For now, only support csv file.

        :param file_name:  default notebook name
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
        with open(abs_path, "w", newline="", encoding=encoding) as f:
            writer = csv.writer(f)

            res = self._fetch_result(start_over=True)
            res = res.json()["result"]
            lst_data = [[normalize("NFKC", unescape(s))
                         if isinstance(s, str) else s
                         for s in row]
                        for row in res["data"]]

            lst_metadata = [m["name"].rpartition(".")[2]
                            for m in res["meta"]]

            writer.writerow(lst_metadata)
            writer.writerows(lst_data)

            while res["has_more"]:
                res = self._fetch_result(start_over=False)
                res = res.json()["result"]
                lst_data = [[normalize("NFKC", unescape(s))
                             if isinstance(s, str) else s
                             for s in row]
                            for row in res["data"]]

                writer.writerows(lst_data)
