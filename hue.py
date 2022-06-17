import copy
import csv
import json
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
from .decorators import retry, ensure_login, ensure_active_session

__all__ = ["Notebook"]

MAX_LEN_PRINT_SQL = 50


class Notebook(requests.Session):
    """
    Hue Notebook API
    An intergraded hiveql platform

    Parameters：
    username: str, default None
        Hue usexrname, if not provided here, user need to call self.login manually
    password: str, Hue password, default None
        Hue password, if not provided here, user need to call self.login manually
    base_url: str, default None
        link to Hue server, default to BASE_URL
    name: str, default ""
        name of Hue notebook
    description: str, default ""
        description of Hue notebook
    hive_settings: dict, default PERFORMANT_SETTINGS
        if you insist hive default settings, set this parameter to None
        if not provided, notebook would use PERFORMANT_SETTINGS
    verbose: bool, default False
        whether to print log on stdout, default False
    """

    BASE_URL = "http://10.19.185.29:8889"

    PERFORMANCE_SETTINGS = {
        "hive.execution.engine": "tez",
        "hive.exec.parallel.thread": "true",
        "hive.exec.dynamic.partition.mode": "nonstrict",
        "hive.vectorized.execution.reduce.enabled": "true",
        "hive.tez.auto.reducer.parallelism": "true",
        }

    def __init__(self,
                 username: str = None,
                 password: str = None,
                 base_url: str = None,
                 name: str = "",
                 description: str = "",
                 hive_settings=PERFORMANCE_SETTINGS,
                 verbose: bool = False):

        self.name = name
        self.description = description
        self.hive_settings = hive_settings
        self.verbose = verbose

        self._set_log(name=name, verbose=verbose)

        if base_url is None:
            self.base_url = self.BASE_URL
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

    def _set_log(self, name, verbose):
        self.log = logging.getLogger(__name__ + f".Notebook[{name}]")
        if len(self.log.handlers) == 0:
            if verbose:
                logger.setup_stdout_level(self.log, logging.INFO)
            else:
                logger.setup_stdout_level(self.log, logging.WARNING)
        else:
            for handler in self.log.handlers:
                if isinstance(handler, logging.StreamHandler):
                    if verbose:
                        handler.setLevel(logging.INFO)
                    else:
                        handler.setLevel(logging.WARNING)

    def login(self, username: str = None, password: str = None):
        self.is_logged_in = False

        self.username = username or self.username
        self._password = password or self._password
        if self.username is None and self._password is None:
            raise ValueError("please provide username and password")

        if self.username is None or self._password is None:
            raise KeyError("username must be specified with password")

        self.log.info(f"logging in for user: [{self.username}]")
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
        if res.status_code != 200:
            self.log.exception('login failed for user [%s] at %s'
                               % (self.username, self.base_url))
        else:
            self.log.info('login succeeful [%s] at %s'
                          % (self.username, self.base_url))

            self.is_logged_in = True
            self.headers["X-CSRFToken"] = self.cookies['csrftoken']
            self.headers["Content-Type"] = "application/x-www-form-urlencoded; " \
                                           "charset=UTF-8"
            self.headers["X-Requested-With"] = "XMLHttpRequest"

            self._prepare_notebook(self.name, self.description)

        return self

    def beeswax(self, query, database='buffer_fk', approx_time=5, attempt_times=100):
        self.log.info(f"beeswax sending query: {query[: MAX_LEN_PRINT_SQL]}")
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
        self.log.info(f"fetching beeswax table detail: {database}.{table_name}")
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

    @retry()
    @ensure_login
    def _create_notebook(self, name="", description=""):
        self.log.info("creating notebook")
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
        r_json = res.json()
        self.notebook = r_json["notebook"]
        self.notebook["name"] = name
        self.notebook["description"] = description
        return res

    @retry()
    @ensure_login
    def _create_session(self):
        self.log.info("creating session")
        url = self.base_url + "/notebook/api/create_session"
        self.headers["Host"] = "10.19.185.29:8889"
        self.headers["Referer"] = "http://10.19.185.29:8889/hue/editor/?type=hive"

        if not hasattr(self, "notebook"):
            self._create_notebook()

        payload = {
            "notebook": json.dumps({
                "id": None,
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

        self._session_time = time.perf_counter()
        return r

    def _prepare_notebook(self, name="", description=""):
        self.log.info("preparing notebook")
        self._create_notebook(name, description)

        self._create_session()
        self.notebook["sessions"] = [self.session]

        if self.hive_settings is not None:
            self.log.info("setting up hive job")
            for key, val in self.hive_settings.items():
                self.execute(f"SET {key}={val};")

    def _prepare_snippet(self, sql: str = "", database="default"):
        self.log.info("preparing snippet")
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
                    "settings": [],
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

    def execute(self,
                sql: str,
                database: str = "default",
                sync=True):

        if hasattr(self, "snippet"):
            self._close_statement()

        self._prepare_snippet(sql, database)
        self.notebook["snippets"] = [self.snippet]

        r_json = self._execute(sql).json()
        if r_json["status"] == -1:
            self.log.error(f"server returned wrong status, response: {r_json}")
            self._close_session()
            r_json = self._execute(sql).json()

        self.notebook = self.notebook.copy()
        self.notebook["id"] = r_json["history_id"]
        self.notebook["uuid"] = r_json["history_uuid"]
        self.notebook["isHistory"] = True
        self.notebook["isBatchable"] = True

        self.snippet = self.snippet.copy()
        self.snippet["result"]["handle"] = r_json["handle"]
        self.snippet["status"] = "running"

        if r_json["status"] == 1:
            self.log.exception(r_json["message"])
            raise RuntimeError(r_json["message"])

        self._result = NotebookResult(self)
        if sync:
            self._result.await_result()

        return self._result

    @retry()
    @ensure_active_session
    @ensure_login
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

        self._session_time = time.perf_counter()
        return res

    def set_priority(self, priority: str):
        """
        Set the priority for Hive Query

        :param priority: Enumerate in "VERY_HIGH", "HIGH", "NORMAL", "LOW", "VERY_LOW"

        """
        self.execute(f"SET mapreduce.job.priority={priority.upper()}")

    @retry()
    @ensure_login
    def _close_statement(self):
        self.log.info(f"closing statement")
        url = self.base_url + "/notebook/api/close_statement"
        res = self.post(url,
                        data={"notebook": json.dumps(self.notebook),
                              "snippet": json.dumps(self.snippet)},
                        )
        self.log.debug(f"close statement response: {res.text}")
        return res

    @retry()
    @ensure_login
    def _close_session(self):
        self.log.info(f"closing session")
        url = self.base_url + "/notebook/api/close_session/"
        res = self.post(url,
                        data={"session": json.dumps(self.session)}
                        )
        self.log.debug(f"close session response: {res.text}")
        return res

    @retry()
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

    @retry()
    def _logout(self):
        self.log.info(f"logging out")

        url = self.base_url + "/accounts/logout/"
        res = self.get(url)
        return res

    def new_notebook(self,
                     name="", description="",
                     hive_settings=PERFORMANCE_SETTINGS,
                     verbose: bool = None):
        new_nb = copy.deepcopy(self)

        new_nb.name = name
        new_nb.description = description
        new_nb.base_url = self.base_url
        new_nb.hive_settings = hive_settings
        new_nb.username = self.username
        new_nb.is_logged_in = self.is_logged_in
        new_nb.verbose = verbose or self.verbose

        new_nb._set_log(name=name, verbose=verbose)
        new_nb._prepare_notebook(name, description)

        return new_nb

    @retry()
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
        self.notebook = notebook.notebook
        self.snippet = notebook.snippet
        self.is_logged_in = notebook.is_logged_in
        self.verbose = notebook.verbose

        self._notebook = notebook

        # the proxy might fail to respond when the response body becomes too large
        # manually set it smaller if so
        self.rows_per_fetch = 65535

        self.log = logging.getLogger(__name__ + f".NotebookResult[{notebook.name}]")
        if len(self.log.handlers) == 0:
            if self.verbose:
                logger.setup_stdout_level(self.log, logging.INFO)
            else:
                logger.setup_stdout_level(self.log, logging.WARNING)
        else:
            for handler in self.log.handlers:
                if isinstance(handler, logging.StreamHandler):
                    if self.verbose:
                        handler.setLevel(logging.INFO)
                    else:
                        handler.setLevel(logging.WARNING)

    @retry()
    def check_status(self):
        self.log.info(f"checking status")
        url = self.base_url + "/notebook/api/check_status"
        res = self._notebook.post(url,
                                  data={"notebook": json.dumps({"id": self.notebook["uuid"]})}
                                  )
        self.log.debug(f"check session response: {res.text}")
        r_json = res.json()
        if r_json["status"] != 0:
            self.log.exception("check status response throws exception: "
                               + r_json["message"])
            raise RuntimeError(r_json["message"])

        status = r_json["query_status"]["status"]
        if status == "running":
            self._notebook._session_time = time.perf_counter()

        self.snippet["status"] = status
        return res

    def await_result(self, attempts: int = float("inf"), wait_sec: int = 3):
        i = 1
        start_time = time.perf_counter()
        while i < attempts:
            msg = f"({i}/{attempts})" if not attempts == float("inf") else ""
            msg = f"awaiting result " \
                      f"elapsed {time.perf_counter() - start_time:.2f} secs" \
                  + msg
            self.log.debug(msg)

            self.check_status()
            if self.snippet["status"] == "available":
                self.log.info(f"sql execution done in {time.perf_counter() - start_time:.2f} secs")
                return

            i += 1
            time.sleep(wait_sec)

        sql = self.snippet["statement"]
        self.log.warning(
            f"result not ready for sql: "
            f"{sql[: MAX_LEN_PRINT_SQL] + '...' if len(sql) > MAX_LEN_PRINT_SQL else sql}"
            )

    @property
    def is_ready(self):
        self.check_status()
        return self.snippet["status"] == "available"

    @retry()
    def _fetch_result(self, rows: int = None, start_over=False):
        self.log.info(f"fetching result")
        if not self.snippet["status"] == "available":
            raise AssertionError("result not ready yet")

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
            res = res.json()["result"]
            lst_data.extend([[normalize("NFKC", unescape(s))
                              if isinstance(s, str) else s
                              for s in row]
                             for row in res["data"]])

        self.data = {"data": lst_data, "columns": lst_metadata}
        return self.data

    def to_csv(self, file_name: str = None, encoding="utf-8"):
        """
        Download result of executed sql directly into a csv file.
        For now, only support csv file.

        :param file_name:  default notebook name
        :return: None
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
