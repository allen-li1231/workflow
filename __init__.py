"""
@Author: 李中豪    supermrli@hotmail.com
"""
import re
import time
import logging
import numpy as np
import pandas as pd

from .hue import Notebook
from .settings import MAX_LEN_PRINT_SQL, HIVE_PERFORMANCE_SETTINGS
from .hue_download import HueDownload
from . import logger

__all__ = ["hue", "Notebook", "HueDownload"]


class hue:
    def __init__(self, username: str, password: str = None,
                 name="", description="",
                 hive_settings=HIVE_PERFORMANCE_SETTINGS,
                 verbose=False):

        # global hue_sys, download
        if password is None:
            print("Please provide password:", end='')
            password = input("")

        self.name = name
        self.description = description
        self.hive_settings = hive_settings
        self.verbose = verbose

        self._set_log(verbose)
        self.hue_sys = Notebook(username, password,
                                name=name,
                                description=description,
                                hive_settings=hive_settings,
                                verbose=False)
        self.hue_download = HueDownload(username, password, verbose)

        self.notebook_workers = [self.hue_sys]

    def _set_log(self, verbose):
        self.log = logging.getLogger(__name__ + f".hue")
        has_stream_handler = False
        for handler in self.log.handlers:
            if isinstance(handler, logging.StreamHandler):
                has_stream_handler = True
                if verbose:
                    handler.setLevel(logging.INFO)
                else:
                    handler.setLevel(logging.WARNING)

        if not has_stream_handler:
            if verbose:
                logger.setup_stdout_level(self.log, logging.INFO)
            else:
                logger.setup_stdout_level(self.log, logging.WARNING)

    def run_sql(self,
                sql: str,
                database: str = "default",
                sync=True,
                print_log: bool = False,
                new_notebook=False):
        """
        sql 查询语句
        database 选填，默认'default'
        sync 选填，True，是否异步执行sql
        :param sql: query raw string to execute
        :param database: database on Hive
                         default to 'default'
        :param sync: whether to wait for sql to complete
                     default to True
        :param print_log: whether to print Yarn log during waiting
                          default to False
        :param new_notebook: whether to initialize a new notebook
                             default to False
        :return: hue.NotebookResult, which handles result of corresponding sql
        """
        if new_notebook:
            nb = self.hue_sys.new_notebook(self.name,
                                           self.description,
                                           self.hive_settings,
                                           verbose=self.hue_sys.verbose)
        else:
            nb = self.hue_sys

        return nb.execute(sql, database, print_log, sync)

    def run_notebook_sql(self, *args, **kwargs):
        return self.run_sql(*args, **kwargs)

    def run_sqls(self,
                 sqls,
                 database="default",
                 n_jobs=3,
                 wait_sec=3,
                 sync=True):
        """
        run concurrent hiveql using Hue Notebook api.

        :param sqls: iterable instance of sql strings
        :param database: string, default "default", database name
        :param n_jobs: number of concurrent queries to run, recommend not greater than 3,
                       otherwise it would sometimes causes "Too many opened sessions" error
        :param wait_sec: wait seconds between each submission of query
        :param sync: whether to wait for all queries to complete
        :return: list of NotebookResults
        """

        while len(self.notebook_workers) < len(sqls):
            self.notebook_workers.append(self.hue_sys
                                         .new_notebook(self.name + f"-worker-{len(self.notebook_workers)}",
                                                       self.description,
                                                       hive_settings=None,
                                                       recreate_session=False,
                                                       verbose=self.hue_sys.verbose))
        d_future = {}
        lst_result = [None] * len(sqls)
        i = 0
        while i < len(sqls) or len(d_future) > 0:
            for notebook, idx in list(d_future.items()):
                try:
                    if sync and not notebook._result.is_ready():
                        continue

                    lst_result[idx] = notebook._result
                    del d_future[notebook]
                except Exception as e:
                    self.log.warning(e)
                    sql = sqls[idx]
                    self.log.warning(
                        f"due to fetch_result exception above, "
                        f"result of the following sql is truncated: "
                        f"{sql[: MAX_LEN_PRINT_SQL] + '...' if len(sql) > MAX_LEN_PRINT_SQL else sql}")
                    del d_future[notebook]

            while i < len(sqls) and (len(d_future) < n_jobs or not sync):
                worker = self.notebook_workers[i]
                try:
                    worker.execute(sqls[i],
                                   database=database,
                                   sync=False)
                    d_future[worker] = i
                except Exception as e:
                    self.log.warning(e)
                    self.log.warning(
                        f"due to execute exception above, "
                        f"result of the following sql is truncated: "
                        f"{sqls[i][: MAX_LEN_PRINT_SQL] + '...' if len(sqls[i]) > MAX_LEN_PRINT_SQL else sqls[i]}")
                finally:
                    i += 1

            time.sleep(wait_sec)

        return lst_result

    def run_notebook_sqls(self, *args, **kwargs):
        return self.run_sqls(*args, **kwargs)

    def download_data(self, *args, **kwargs):
        return self.hue_download.download_data(*args, **kwargs)

    def download(self,
                 table: str,
                 reason: str,
                 columns: list = None,
                 column_names: str = ' ',
                 decrypt_columns: list = None,
                 limit: int = None,
                 path: str = None,
                 wait_sec: int = 5,
                 timeout: float = float("inf")
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
        :param limit: the maximum number of records to be downloaded
                      default to all records
        :param path: output csv file if specified.
                     default to return Pandas.DataFrame
                     this is designed to download large table without using up memory
        :param wait_sec: time interval while waiting server for preparing for download
                         default to 5 seconds
        :param timeout: maximum seconds to wait for the server preparation
                       default to wait indefinitely
        :return: Pandas.DataFrame if path is not specified,
                 otherwise output a csv file to path and return None
        """
        return self.hue_download.download(
            table,
            reason,
            columns,
            column_names,
            decrypt_columns,
            limit,
            path,
            wait_sec,
            timeout)

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
               table_name: str = None
               ):
        uploaded_table = self.hue_download.upload(data=data,
                                                  reason=reason,
                                                  columns=columns,
                                                  column_names=column_names,
                                                  encrypt_columns=encrypt_columns,
                                                  wait_sec=wait_sec,
                                                  timeout=timeout)
        if table_name is None:
            self.log.info('data has uploaded to table ' + uploaded_table)
            return uploaded_table

        try:
            self.run_sql('ALTER TABLE %s RENAME TO %s' % (uploaded_table, table_name))
            self.log.info('data has uploaded to the table ' + table_name)
            return table_name
        except Exception as e:
            self.log.warning(e)
            self.log.info('data has uploaded to the table ' + uploaded_table)
            return uploaded_table

    def insert_data(self, file_path, table_name, reason, uploadColumnsInfo='1', uploadEncryptColumns=''):
        """
            file_path  必填，需要上传文件位置
            table_name 必填，需要插入数据的表名
            reason 必填，上传事由
            uploadColumnsInfo 选填，默认写1，可用作备注，与上传数据无关
            uploadEncryptColumns 选填，默认'',需要加密的列，多个用逗号隔开
        """
        uploaded_table = self.hue_download.upload_data(file_path=file_path,
                                                       reason=reason,
                                                       uploadColumnsInfo=uploadColumnsInfo,
                                                       uploadEncryptColumns=uploadEncryptColumns)
        try:
            self.run_sql('insert into table %s \nselect * from %s' % (table_name, uploaded_table))
            print('success')
        except Exception as e:
            print('upload failed, the data is uploaded to the table ' + uploaded_table)
            return uploaded_table

    def get_table(self,
                  table,
                  columns=None,
                  database: str = "default",
                  decrypt_columns: list = None,
                  print_log: bool = False):
        """
        get data from Hue to local
        :param table: table name on Hue
        :param columns: iterable instance of string of column names
                        default to all columns
        :param database: string, default "default", database name
        :param decrypt_columns: columns to be decrypted
        :param print_log: whether to print Yarn log during waiting
                          default to False
        :return: Pandas.DataFrame
        """
        if decrypt_columns is None:
            sql = f"select {','.join(columns) if columns else '*'} from {table};"
            res = self.run_sql(sql=sql,
                               database=database,
                               print_log=print_log)
            return pd.DataFrame(**res.fetchall())
        else:
            return self.download(table=table,
                                 reason=table,
                                 columns=columns,
                                 decrypt_columns=decrypt_columns)

    def kill_app(self, app_id):
        return self.hue_download.kill_app(app_id)

    def close(self):
        for worker in self.notebook_workers:
            worker.close()

        self.hue_sys.logout()
