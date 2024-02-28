import os
import time
import logging
import getpass
import pandas as pd
from tqdm import tqdm
from typing import Iterable

from .compat import HiveServer2CompatCursor, _in_old_env
from ..logger import set_stream_log_level
# from ..utils import get_ip
from ..settings import (HIVESERVER_IP, HIVESERVER_PORT,
                        HIVECLI_MAX_CONCURRENT_SQL, MAX_LEN_PRINT_SQL,
                        PROGRESSBAR, HIVE_PERFORMANCE_SETTINGS)


class HiveClient:
    def __init__(self,
                #  env='zh',
                 auth: dict = None,
                 database: str = None,
                 config: dict = None,
                 verbose=False
                 ):

        self.log = logging.getLogger(__name__ + f".HiveClient")
        set_stream_log_level(self.log, verbose=verbose)
        self.auth = auth if isinstance(auth, dict) else {
            'host': HIVESERVER_IP,
            'port': HIVESERVER_PORT,
            'user': input('Please provide Hive username:'),
            'password': getpass.getpass('Please provide Hive password:'),
            'auth_mechanism': 'PLAIN'
        }
        # ip = get_ip()
        # if env == 'zh':
        #     auth = {
        #         'host': VULCAN_ZH_IP if ip.startswith("10.212") else VULCAN_ZH_ROUTER_IP,
        #         'port': 10000,
        #         'user': input('请输入 Hive 用户名:'),
        #         'password': getpass.getpass('请输入 Hive 密码:'),
        #         'auth_mechanism': 'PLAIN'
        #     }
        # elif env == 'mex':
        #     auth = {
        #         'host': VULCAN_MEX_IP if ip.startswith("10.212") else VULCAN_MEX_ROUTER_IP,
        #         'port': 10000,
        #         'user': 'vulcan-x',
        #         'password': 'vulcan-x',
        #         'auth_mechanism': 'PLAIN'
        #     }
        # else:
        #     raise ValueError("env name `{}` currently not supported ".format(env))

        # self.env = env
        self.config = HIVE_PERFORMANCE_SETTINGS.copy() if config is None else config
        self._auth = auth
        self._workers = [
            HiveServer2CompatCursor(
                **self._auth,
                database=database,
                config=config,
                name="HiveClient-worker-0",
                verbose=verbose)
        ]

    @property
    def cursor(self):
        return self._workers[0]

    def set_batch_size(self, size):
        self.log.debug(f"Set cursor set_arraysize to {size}")
        for worker in self._workers:
            worker.set_arraysize(size)

    def _fetch_df(self, cursor):
        self.log.debug(f"Fetch and output pandas dataframe")
        if _in_old_env:
            res = cursor.fetchall()
            df = pd.DataFrame(res, copy=False)

            if len(res) > 0:
                df.columns = [col.split('.')[-1] for col in res[0].keys()]
        elif cursor.has_result_set:
            from impala.util import as_pandas
            df = as_pandas(cursor)
            df.columns = [col.split('.')[-1] for col in df.columns]
        else:
            return pd.DataFrame()

        return df
    
    def update_hive_config(self, config: dict = None, **kwargs):
        if isinstance(config, dict):
            self.config.update(config)
        elif kwargs:
            self.config.update(kwargs)
        else:
            raise ValueError(f"expect dict or keyword argument, got {config}")

    def remove_hive_config(self, key: str):
        if key in self.config:
            del self.config[key]

    def run_hql(self, sql: str, param=None, config=None, verbose=True, sync=True):
        config = config.copy() if isinstance(config, dict) else self.config

        # thread unsafe
        user_engine = None
        if sql.lower().count("union all") >= 3 and isinstance(config, dict):
            self.log.debug(f"Detect multiple tables unioned, fallback to mr engine")
            user_engine = config.get("hive.execution.engine", "mr")
            if user_engine != "mr":
                config["hive.execution.engine"] = "mr"

    
        self.cursor.execute_async(sql, parameters=param, configuration=config)

        if isinstance(user_engine, str):
            config["hive.execution.engine"] = user_engine

        if sync:
            self.cursor._wait_to_finish(verbose=verbose)
            return self._fetch_df(self.cursor)

    def run_hqls(self,
                 sqls,
                 param=None,
                 config=None,
                 n_jobs=HIVECLI_MAX_CONCURRENT_SQL,
                 wait_sec=0.,
                 progressbar=True,
                 progressbar_offset=0,
                 sync=True
                 ):
        """
        run concurrent HiveQL using impyla api.

        :param sqls: iterable instance of sql strings,
            or single sql string containing multiple sqls seperated by ';'
        :param param: tuple of two strings, parameter tuple[0] will be replaced by value tuple[1]
        :param n_jobs: number of concurrent queries to run, it is recommended not greater than 4,
                       otherwise it would sometimes causes "Too many opened sessions" error
        :param wait_sec: wait seconds between submission of query
        :param progressbar: whether to show progress bar during waiting
        :param progressbar_offset: use this parameter to control sql progressbar positions
        :param sync: whether to wait for all queries to complete execution

        :return: list of pandas dataframe results
        """

        if isinstance(sqls, str):
            sqls = [s for s in sqls.split(";") if len(s.strip()) > 0]

        # setup logging level and log file
        while len(self._workers) < len(sqls):
            name=f"HiveClient-worker-{len(self._workers)}"
            self._workers.append(
                self.cursor.copy(
                    user=self.cursor.user, config=self.cursor.config,
                    name=name, log_file_path=os.path.join(os.getcwd(), f"{name}.log")
                )
            )

        # go for concurrent sql run
        i = 0
        d_future = {}
        lst_result = [None] * len(sqls)
        if progressbar:
            setup_pbar = PROGRESSBAR.copy()
            if "desc" in setup_pbar:
                del setup_pbar["desc"]
            pbar = tqdm(total=len(sqls), desc="run_hqls progress",
                position=progressbar_offset, **setup_pbar)

        while i < len(sqls) or len(d_future) > 0:
            # check and collect completed results
            for worker, idx in list(d_future.items()):
                try:
                    is_finished = worker._check_operation_status(verbose=False)
                    if sync and not is_finished:
                        continue

                    lst_result[idx] = self._fetch_df(worker)
                    del d_future[worker]
                    if progressbar:
                        pbar.update(1)

                except Exception as e:
                    self.log.warning(e)
                    sql = sqls[idx]
                    self.log.warning(
                        f"due to fetch_result exception above, "
                        f"result of the following sql is truncated: "
                        f"{sql[: MAX_LEN_PRINT_SQL] + '...' if len(sql) > MAX_LEN_PRINT_SQL else sql}")
                    lst_result[idx] = e
                    del d_future[worker]
                    if progressbar:
                        pbar.update(1)

            # add task to job pool when there exists vacancy
            while i < len(sqls) and (len(d_future) < n_jobs or not sync):
                worker = self._workers[i]
                try:
                    p = param[i] if isinstance(param, Iterable) else param
                    c = config[i] if isinstance(config, Iterable) else config
                    worker.execute_async(sqls[i], parameters=p, configuration=c)
                    d_future[worker] = i
                except Exception as e:
                    self.log.warning(e)
                    self.log.warning(
                        f"due to execute exception above, "
                        f"result of the following sql is truncated: "
                        f"{sqls[i][: MAX_LEN_PRINT_SQL] + '...' if len(sqls[i]) > MAX_LEN_PRINT_SQL else sqls[i]}")
                    lst_result[i] = e
                    if progressbar:
                        pbar.update(1)
                finally:
                    i += 1

            time.sleep(wait_sec)

        if progressbar:
            pbar.close()

        return lst_result

    def run_hql_file(self,
                     file_path,
                     encoding='utf-8',
                     concurrent=False,
                     param=None,
                     config=None,
                     sync=True
                     ):

        with open(file_path, 'r', encoding=encoding) as f:
            sql_text = f.read().strip()

        if concurrent:
            return self.run_hqls(sql_text, param=param, config=config, sync=sync)
        else:
            return self.run_hql(sql_text, param=param, config=config, sync=sync)

    def close(self):
        for worker in self._workers:
            worker.close()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __enter__(self):
        return self
