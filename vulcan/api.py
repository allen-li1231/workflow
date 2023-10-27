import time
import logging
import getpass
import pandas as pd
from tqdm import tqdm
from typing import Iterable

from .compat import HiveServer2CompatCursor, _in_old_env
from ..logger import set_stream_log_level
from ..utils import get_ip
from ..settings import (VULCAN_ZH_IP, VULCAN_MEX_IP,
                       VULCAN_ZH_ROUTER_IP, VULCAN_MEX_ROUTER_IP,
                       VULCAN_CONCURRENT_SQL, MAX_LEN_PRINT_SQL,
                       PROGRESSBAR, HIVE_PERFORMANCE_SETTINGS)


class HiveClient:
    def __init__(self, env='zh', database: str = None, config: dict = None, verbose=False):
        self.log = logging.getLogger(__name__ + f".HiveClient")
        set_stream_log_level(self.log, verbose=verbose)

        ip = get_ip()
        if env == 'zh':
            auth = {
                'host': VULCAN_ZH_IP if ip.startswith("10.212") else VULCAN_ZH_ROUTER_IP,
                'port': 10000,
                'user': input('请输入 Hive 用户名:'),
                'password': getpass.getpass('请输入 Hive 密码:'),
                'auth_mechanism': 'PLAIN'
            }
        elif env == 'mex':
            auth = {
                'host': VULCAN_MEX_IP if ip.startswith("10.212") else VULCAN_MEX_ROUTER_IP,
                'port': 10000,
                'user': 'vulcan-x',
                'password': 'vulcan-x',
                'auth_mechanism': 'PLAIN'
            }
        else:
            raise ValueError("env name `{}` currently not supported ".format(env))

        self.env = env
        self.config = HIVE_PERFORMANCE_SETTINGS if config is None else config
        self._auth = auth
        self._workers = [
            HiveServer2CompatCursor(**auth, database=database, config=config, verbose=verbose)
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
        else:
            from impala.util import as_pandas
            df = as_pandas(cursor)
            df.columns = [col.split('.')[-1] for col in df.columns]

        return df

    def run_hql(self, sql: str, param=None, config=None, verbose=True, sync=True):
        config = config.copy() if isinstance(config, dict) else self.config

        # thread unsafe
        user_engine = None
        if sql.lower().count("union all") >= 3 and isinstance(config, dict):
            self.log.debug(f"Detect large table unioned, fallback to mr engine")
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
                 n_jobs=VULCAN_CONCURRENT_SQL,
                 wait_sec=1,
                 progressbar=True,
                 progressbar_offset=0,
                 desc: str="run_hqls progress",
                 sync=True
                 ):
        """
        run concurrent HiveQL using impyla api.

        :param sqls: iterable instance of sql strings
        :param database: string, default "default", database name
        :param n_jobs: number of concurrent queries to run, it is recommended not greater than 4,
                       otherwise it would sometimes causes "Too many opened sessions" error
        :param wait_sec: wait seconds between submission of query
        :param progressbar: whether to show progress bar during waiting
        :param progressbar_offset: use this parameter to control sql progressbar positions
        :param sync: whether to wait for all queries to complete execution

        :return: list of NotebookResults
        """

        if isinstance(sqls, str):
            sqls = [s for s in sqls.split(";") if len(s.strip()) > 0]

        # setup logging level
        while len(self._workers) < len(sqls):
            self._workers.append(self.cursor.copy())

        # go for concurrent sql run
        i = 0
        d_future = {}
        lst_result = [None] * len(sqls)
        if progressbar:
            setup_pbar = PROGRESSBAR.copy()
            del setup_pbar["desc"]
            pbar = tqdm(total=len(sqls), desc=desc,
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

    def close(self):
        for worker in self._workers:
            worker.close()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __enter__(self):
        return self
