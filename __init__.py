"""
@Author: Allen Li    supermrli@hotmail.com
"""
import os
import time
from typing import Union
import logging
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd

from .hue import Notebook
from .settings import MAX_LEN_PRINT_SQL, HIVE_PERFORMANCE_SETTINGS, PROGRESSBAR, EXCEL_ENGINE
from .hue_download import HueDownload
from .utils import append_df_to_csv
from . import logger

__all__ = ["hue", "Notebook", "HueDownload"]


class hue:
    def __init__(self, username: str, password: str = None,
                 name="", description="",
                 hive_settings=None,
                 verbose=False):

        # global hue_sys, download
        if password is None:
            print("Please provide password:", end='')
            password = input("")

        self.username = username
        self.name = name
        self.description = description
        self.hive_settings = HIVE_PERFORMANCE_SETTINGS.copy() \
            if hive_settings is None else hive_settings
        self.verbose = verbose
        self.log = logging.getLogger(__name__ + ".hue")
        if self.verbose:
            logger.set_stream_log_level(self.log, verbose=verbose)

        self.hue_sys = Notebook(username, password,
                                name=name,
                                description=description,
                                hive_settings=hive_settings,
                                verbose=False)
        self.hue_download = HueDownload(username, password, verbose)

        self.notebook_workers = [self.hue_sys]

    def run_sql(self,
                sql: str,
                database: str = "default",
                print_log: bool = False,
                progressbar: bool = True,
                progressbar_offset: int = 0,
                sync=True,
                new_notebook=False):
        """
        Run HiveQL using hue Notebook API

        :param sql: query raw string to execute
        :param database: database on Hive
                         default to 'default'
        :param print_log: whether to print cloud during waiting
                          default to False
        :param progressbar: whether to print progressbar during waiting
                          default to True
        :param progressbar_offset: use this parameter to control sql progressbar positions
        :param sync: whether to wait for sql to complete
                     default to True
        :param new_notebook: whether to initialize a new notebook
                             default to False

        :return: hue.NotebookResult, which handles result of corresponding sql query
        """
        if new_notebook:
            nb = self.hue_sys.new_notebook(self.name,
                                           self.description,
                                           self.hive_settings,
                                           verbose=self.hue_sys.verbose)
        else:
            nb = self.hue_sys

        return nb.execute(sql,
                          database=database,
                          print_log=print_log,
                          progressbar=progressbar,
                          progressbar_offset=progressbar_offset,
                          sync=sync)

    def run_notebook_sql(self, *args, **kwargs):
        return self.run_sql(*args, **kwargs)

    def run_sqls(self,
                 sqls,
                 database="default",
                 n_jobs=3,
                 wait_sec=0,
                 progressbar=True,
                 progressbar_offset=0,
                 sync=True
                 ):
        """
        run concurrent HiveQL using Hue Notebook api.

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

        # setup logging level
        while len(self.notebook_workers) < len(sqls):
            self.notebook_workers.append(
                self.hue_sys.new_notebook(
                    self.name + f"-worker-{len(self.notebook_workers)}",
                    self.description,
                    hive_settings=None,
                    recreate_session=False,
                    verbose=self.hue_sys.verbose)
            )

        # go for concurrent sql run
        i = 0
        d_future = {}
        lst_result = [None] * len(sqls)
        setup_pbar = PROGRESSBAR.copy()

        while i < len(sqls) or len(d_future) > 0:
            # check and collect completed results
            for notebook, idx in list(d_future.items()):
                result = notebook._result
                try:
                    result.check_status()
                    if progressbar:
                        result.update_progressbar(result._progressbar)
                    if sync and not result.is_ready():
                        continue

                    lst_result[idx] = result
                    del d_future[notebook]
                except Exception as e:
                    self.log.warning(e)
                    sql = sqls[idx]
                    self.log.warning(
                        f"due to fetch_result exception above, "
                        f"result of the following sql is truncated: "
                        f"{sql[: MAX_LEN_PRINT_SQL] + '...' if len(sql) > MAX_LEN_PRINT_SQL else sql}")
                    lst_result[idx] = e
                    del d_future[notebook]

            # add task to job pool when there exists vacancy
            while i < len(sqls) and (len(d_future) < n_jobs or not sync):
                worker = self.notebook_workers[i]
                try:
                    result = worker.execute(sqls[i],
                                            database=database,
                                            sync=False)
                    d_future[worker] = i
                    if progressbar:
                        setup_pbar["desc"] = PROGRESSBAR["desc"].format(name=worker.name, result="result")
                        result._progressbar = tqdm(total=100, position=i + progressbar_offset, **setup_pbar)

                except Exception as e:
                    self.log.warning(e)
                    self.log.warning(
                        f"due to execute exception above, "
                        f"result of the following sql is truncated: "
                        f"{sqls[i][: MAX_LEN_PRINT_SQL] + '...' if len(sqls[i]) > MAX_LEN_PRINT_SQL else sqls[i]}")
                    lst_result[i] = e
                finally:
                    i += 1

            time.sleep(wait_sec)

        if progressbar:
            for result in lst_result:
                if hasattr(result, "_progressbar"):
                    result._progressbar.close()

        return lst_result

    def run_notebook_sqls(self, *args, **kwargs):
        return self.run_sqls(*args, **kwargs)

    def download_data(self, *args, **kwargs):
        return self.hue_download.download_data(*args, **kwargs)

    def download(self,
                 table: str,
                 reason: str,
                 columns: list = None,
                 column_names: list = None,
                 decrypt_columns: list = None,
                 path: str = None,
                 n_jobs: int = 10,
                 progressbar: bool = True,
                 progressbar_offset: int = 0,
                 print_log=False,
                 **info_kwargs
                 ):
        """
        a refactored version of download_data from WxCustom
        specify table information and load or download to local

        :param table: table name on Hue (database name is required)
        :param reason:  reason of downloading
        :param columns: specify which of the columns in table to download from Hue,
                        default to all columns
        :param column_names: rename column names if needed
        :param decrypt_columns: columns to be decrypted
        :param path: output file if specified.
                     default to return Pandas.DataFrame without saving data to local
                     when save file in .csv, the method is designed to download large table in low memory
        :param n_jobs: maximum concurrent download tasks
                       default to 10
        :param progressbar: whether to show a progressbar
        :param progressbar_offset: position of tqdm progressbar
        :param print_log: whether to print cloud log to console
        :param info_kwargs: to modify default get_info_by_id parameters, add argument pairs here
                            (useful when downloadables cannot be found in just one page)

        :return: Pandas.DataFrame if path is not specified,
                 otherwise output file to path and return None
        """

        if path:
            dir_path = os.path.dirname(path)
            if not os.path.exists(dir_path):
                raise NotADirectoryError(f"path does not exist: '{path}'")

        table_rows = (self
            .run_sql(f"select count(*) from {table}", progressbar=False)
            .fetchall(progressbar=False)["data"][0][0])
        if not isinstance(table_rows, int) or table_rows <= 1e5:
            return self.hue_download.download(
                table=table,
                reason=reason,
                columns=columns,
                column_names=column_names,
                decrypt_columns=decrypt_columns,
                path=path,
                **info_kwargs)

        self.log.info(f"downloading large table '{table}'. split into chunks and batch download")
        # pre-execute preparation
        database, dot, _ = table.rpartition('.')
        str_tmp_table = f"{database}.{self.username}" if len(dot) else f"{self.username}"
        str_tmp_table += "_tmp_{}"
        str_create_tmp_table = "create table {} like " + table
        str_drop_tmp_table = "drop table if exists {}"
        lst_tmp_tables = []
        lst_create_tmp_table = []
        lst_drop_tmp_table = []
        str_insert_tmp_table_query = f"from (select *, ROW_NUMBER() OVER () as tmp_row from {table}) t"
        for i in range(1, table_rows + 1, 100000):
            tmp_table = str_tmp_table.format(int(time.time() * 1000))
            lst_tmp_tables.append(tmp_table)
            lst_create_tmp_table.append(str_create_tmp_table.format(tmp_table))
            lst_drop_tmp_table.append(str_drop_tmp_table.format(tmp_table))
            str_insert_tmp_table_query += \
                f"\ninsert into table {tmp_table} select `(tmp_row)?+.+` where tmp_row between {i} and {i + 99999}"

            time.sleep(1e-3)

        b_backtick_as_regex = False
        if "hive.support.quoted.identifiers" in self.hue_sys.hive_settings \
                and self.hue_sys.hive_settings["hive.support.quoted.identifiers"] == "none":
            b_backtick_as_regex = True

        self.log.info(f"creating temporary table for '{table}'")
        try:
            self.run_sqls(lst_create_tmp_table, progressbar=False)
            self.hue_sys.set_backtick(as_regex=True)
            self.hue_sys.set_hive("tez.grouping.split-count", str(len(lst_tmp_tables) * 5 + 1))
            self.run_sql(str_insert_tmp_table_query,
                         progressbar=progressbar,
                         progressbar_offset=progressbar_offset,
                         print_log=print_log)

            self.log.info("downloading chunks")
            lst_paths = [f"{path}.wfdl{i}" for i in range(len(lst_tmp_tables))]
            self.batch_download(lst_tmp_tables,
                                reasons=reason,
                                columns=[columns] * len(lst_tmp_tables),
                                decrypt_columns=[decrypt_columns] * len(lst_tmp_tables),
                                paths=lst_paths,
                                n_jobs=n_jobs,
                                progressbar=progressbar,
                                progressbar_offset=progressbar_offset,
                                **info_kwargs)
        except Exception as e:
            self.run_sqls(lst_drop_tmp_table, progressbar=False)
            self.hue_sys.unset_hive("tez.grouping.split-count")
            if not b_backtick_as_regex:
                self.hue_sys.set_backtick(as_regex=False)
            raise e

        self.hue_sys.unset_hive("tez.grouping.split-count")
        if not b_backtick_as_regex:
            self.hue_sys.set_backtick(as_regex=False)

        self.log.info("merging chunks")
        if os.path.isfile(path):
            os.remove(path)

        for excel in lst_paths:
                df = pd.read_csv(excel, encoding="utf-8")
                append_df_to_csv(path, df, encoding="utf-8", index=False)
                os.remove(excel)

        self.log.info("cleaning up caches")
        self.run_sqls(lst_drop_tmp_table, progressbar=False)

    def batch_download(self,
                       tables: list,
                       reasons: Union[str, list] = None,
                       columns: list = None,
                       column_names: list = None,
                       decrypt_columns: list = None,
                       paths: list = None,
                       n_jobs: int = 10,
                       use_hue: bool = False,
                       progressbar: bool = True,
                       progressbar_offset: int = 0,
                       **info_kwargs
                       ):
        """
        Batch downloading tasks

        :param tables: iterable of string of table names
        :param reasons: reasons of downloading, iterable of string of reasons or string
                        when passed string, it indicates all tables are downloaded for one reason
        :param columns: specify which of the columns in table to download from Hue,
                        default to all columns
        :param column_names: rename column names if needed
        :param decrypt_columns: columns to be decrypted
        :param paths: iterable of path string, optional, will download file if passed
                      when save file in .csv, the method is designed to download large table in low memory
        :param n_jobs: maximum concurrent download tasks
                       default to 10
        :param use_hue: whether to fetch data from hue
        :param progressbar: whether to show a progressbar
        :param progressbar_offset: position of tqdm progressbar
        :param info_kwargs: to modify get_info_by_id parameters, add argument pairs here

        :return: Pandas.DataFrame if paths are not specified,
                 otherwise output files to path and return None
        """

        if decrypt_columns is not None \
                and any(decrypt_columns) \
                and reasons is None:
            raise TypeError("must specify reason if there has any table's column needs to decrypt")

        params = [tables,
                  [reasons] * len(tables) if reasons is None or isinstance(reasons, str) else reasons,
                  columns if columns and any(columns) else [None] * len(tables),
                  column_names if column_names and any(column_names) else [None] * len(tables),
                  decrypt_columns if decrypt_columns and any(decrypt_columns) else [None] * len(tables),
                  paths if paths and any(paths) else [None] * len(tables)]

        if use_hue and n_jobs > 10 \
                and ("size" not in info_kwargs
                     or ("size" in info_kwargs and info_kwargs["size"] <= 10)):
            info_kwargs["size"] = n_jobs

        d_future = {}
        with ThreadPoolExecutor(max_workers=n_jobs) as executor:
            for i, (table, reason, cols, col_names, decrypt_cols, path) \
                    in enumerate(zip(*params)):
                d_future[
                    executor.submit(
                        self.get_table,
                        table=table,
                        reason=reason,
                        columns=cols,
                        column_names=col_names,
                        decrypt_columns=decrypt_cols,
                        path=path,
                        use_hue=use_hue,
                        new_notebook=True,
                        progressbar=False,
                        **info_kwargs)
                ] = i

            if progressbar:
                setup_pbar = PROGRESSBAR.copy()
                setup_pbar["desc"] = "batch downloading"
                pbar = tqdm(total=len(d_future), miniters=0, position=progressbar_offset, **setup_pbar)

            # won't work only if returned value is None
            lst_result = [None] * len(d_future)
            for future in as_completed(d_future):
                try:
                    lst_result[d_future[future]] = future.result()
                except Exception as e:
                    self.log.warning(e)
                    self.log.warning(
                        f"due to download exception above, "
                        f"table '{table}' download is cancelled")
                    lst_result[d_future[future]] = future.exception()

                if progressbar:
                    pbar.update(1)

                pbar.refresh()

        if progressbar:
            pbar.close()
        return lst_result

    def upload_data(self, file_path, reason, column_names='1', encrypt_columns='', table_name=None):
        """
            file_path  必填，需要上传文件位置
            reason 必填，上传事由
            uploadColumnsInfo 选填，默认写1，可用作备注，与上传数据无关
            uploadEncryptColumns 选填，默认'',需要加密的列，多个用逗号隔开
            table_name 选填，默认Nnoe，使用自动分配的表名
        """

        uploaded_table = self.hue_download.upload_data(file_path=file_path,
                                                       reason=reason,
                                                       column_names=column_names,
                                                       encrypt_columns=encrypt_columns)
        if table_name is not None:
            try:
                self.run_sql('ALTER TABLE %s RENAME TO %s' % (uploaded_table, table_name))
                self.log.info('file uploaded to the table ' + table_name)
                return table_name
            except Exception as e:
                self.log.warning(e)
                return uploaded_table
        else:
            self.log.info('file has uploaded to table ' + uploaded_table)
            return uploaded_table

    def upload(self,
               data,
               reason: str,
               columns: list = None,
               column_names: list = None,
               encrypt_columns: list = None,
               wait_sec: int = 5,
               timeout: float = float("inf"),
               table_name: str = None,
               **info_kwargs
               ):
        """
        a refactored version of upload_data from WxCustom
        parse upload data and call upload API, if success, return uploaded table name.

        :param data: pandas.DataFrame, pandas.Series or path str to xlsx, xls or csv file
        :param reason: str, upload reason
        :param columns: list, list of columns to upload
        :param column_names: list, list of column with respective to their alias,
                            must be as same length as columns
        :param encrypt_columns: list, list of columns to encrypt during upload
        :param wait_sec: time interval while waiting server for preparing for upload
                         default to 5 seconds
        :param timeout: maximum seconds to wait for the server preparation
                       default to wait indefinitely
        :param table_name: str, user can nominate final table name
        :param info_kwargs: to modify get_info_by_id parameters, add argument pairs here

        :return: str, name of uploaded table
        """

        uploaded_table = self.hue_download.upload(data=data,
                                                  reason=reason,
                                                  columns=columns,
                                                  column_names=column_names,
                                                  encrypt_columns=encrypt_columns,
                                                  wait_sec=wait_sec,
                                                  timeout=timeout,
                                                  **info_kwargs)
        if table_name is None:
            self.log.info('data has uploaded to table ' + uploaded_table)
            return uploaded_table

        try:
            self.run_sql('ALTER TABLE %s RENAME TO %s' % (uploaded_table, table_name))
            self.log.info('data has uploaded to the table ' + table_name)
            return table_name
        except Exception as e:
            self.log.warning(e)
            self.log.warning('data has uploaded to the table ' + uploaded_table)
            return uploaded_table

    def insert_data(self,
                    data,
                    table_name: str,
                    reason: str,
                    columns: list = None,
                    column_names: list = None,
                    encrypt_columns: list = None,
                    drop: bool = True,
                    progressbar: bool = True,
                    progressbar_offset: int = 0
                    ):
        """
        Upload and insert data into existing table

        :param data: pandas.DataFrame, pandas.Series or path str to xlsx, xls or csv file
        :param table_name: name of table to which data will append
        :param columns: list, list of columns to upload
        :param column_names: list, list of column with respective to their alias,
                            must be as same length as columns
        :param encrypt_columns: list, list of columns to encrypt during upload
        :param reason: str, upload reason
        :param drop: whether to drop uploaded temporary table once insertion succeeds
        :param progressbar: whether to show procedure progressbars
        :param progressbar_offset: position of tqdm progressbars

        :return destination table name
        """

        uploaded_table = self.upload(data=data,
                                     reason=reason,
                                     columns=columns,
                                     column_names=column_names,
                                     encrypt_columns=encrypt_columns)
        try:
            self.run_sql(f'insert into table {table_name} select * from {uploaded_table}',
                         progressbar=progressbar,
                         progressbar_offset=progressbar_offset)
        except Exception as e:
            self.log.warning(e)
            self.log.warning('upload failed, the data is uploaded to the table ' + uploaded_table)
            return uploaded_table

        if drop:
            self.run_sql("drop table if exists " + uploaded_table, progressbar=False)

        return table_name

    def get_table(self,
                  table: str,
                  reason: str = None,
                  columns: list = None,
                  column_names: list = None,
                  decrypt_columns: list = None,
                  path: str = None,
                  use_hue: bool = False,
                  new_notebook: bool = False,
                  rows_per_fetch: int = 32768,
                  progressbar: bool = True,
                  progressbar_offset: int = 0,
                  **info_kwargs
                  ):
        """
        get data from Hue to local as pandas dataframe

        :param table: str, table name on Hue
        :param reason: str, upload reason
        :param columns: iterable instance of string of column names
                        default to all columns
        :param column_names: rename column names if needed
        :param decrypt_columns: columns to be decrypted
        :param path: default None, path to save table data, not to save table if None
        :param use_hue: default False, whether try to fetch specified data from hue
        :param new_notebook: default False, whether to open a new Notebook, this is not designed for user use
        :param rows_per_fetch: rows to fetch per request, tweak it if encounter "Too many sessions" error
        :param progressbar: whether to show progress bar during waiting
        :param progressbar_offset: use this parameter to control sql progressbar positions
        :param info_kwargs: to modify get_info_by_id parameters, add argument pairs here

        :return: Pandas.DataFrame
        """

        if path:
            dir_path = os.path.dirname(path)
            if not os.path.exists(dir_path):
                self.log.error(f"directory '{dir_path}' does not exist")
                raise NotADirectoryError(f"directory '{dir_path}' does not exist")
            suffix = os.path.basename(path).rpartition('.')[-1]

        if use_hue and (decrypt_columns is None or len(decrypt_columns) == 0):
            sql = f"select {','.join(columns) if columns else '*'} from {table};"
            res = self.run_sql(sql=sql,
                               progressbar=progressbar,
                               progressbar_offset=progressbar_offset,
                               print_log=False,
                               new_notebook=new_notebook)
            res.rows_per_fetch = rows_per_fetch
            if progressbar:
                table_size = (self.run_sql(f'show tblproperties {table}("numRows")',
                                           progressbar=False,
                                           progressbar_offset=progressbar_offset,
                                           print_log=False,
                                           new_notebook=new_notebook)
                    .fetchall(progressbar=False)["data"][0][0])
            df = pd.DataFrame(**res.fetchall(total=int(table_size) if progressbar and table_size.isdigit() else None,
                                             progressbar=progressbar,
                                             progressbar_offset=progressbar_offset))
            if column_names:
                if len(df.columns) != len(column_names):
                    self.log.warning(f"length of table columns({len(columns)}) "
                                     f"mismatch with column_names({len(column_names)}), rename skipped")
                else:
                    df.columns = column_names
            if path:
                if suffix in ('xlsx', 'xls', 'xlsm'):
                    df.to_excel(path, index=False, engine=EXCEL_ENGINE)
                else:
                    df.to_csv(path, index=False)
            return df
        elif reason is None:
            raise TypeError(f"must specify reason if there has column to be decrypted")
        else:
            return self.hue_download.download(table=table,
                                              reason=reason,
                                              columns=columns,
                                              column_names=column_names,
                                              decrypt_columns=decrypt_columns,
                                              path=path,
                                              **info_kwargs)

    def kill_app(self, app_id):
        """
        Kill Yarn Application by app id accordingly

        :param app_id: str or iterable of app_ids
        """

        return self.hue_download.kill_app(app_id)

    def close(self):
        for worker in self.notebook_workers:
            worker.close()

        self.hue_sys.logout()
