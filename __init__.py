"""
@Author: 王兴
         李中豪    supermrli@hotmail.com
"""
import json
import os
import re
from concurrent.futures import ThreadPoolExecutor

import numpy as np

from .hue import Notebook, Beeswax, MAX_LEN_PRINT_SQL, PERFORMANCE_SETTINGS
from .hue_download import Hue_download

__all__ = ["hue", "Notebook", "Hue_download"]


class hue:
    def __init__(self, username: str, password: str,
                 name="", description="",
                 hive_settings=PERFORMANCE_SETTINGS,
                 verbose=False):

        # global hue_sys, download
        if password is None:
            print("Please provide password:", end='')
            password = input("")

        self.name = name
        self.description = description
        self.hive_settings = hive_settings
        self.verbose = verbose

        self.hue_sys = Notebook(name=name,
                                description=description,
                                hive_settings=hive_settings,
                                verbose=verbose)
        self.beeswax = Beeswax()
        self.download = Hue_download()
        self.download.username = username
        self.download.password = password
        self.hue_sys.login(username, password)
        self.beeswax.login(username, password)
        self.download.login()

        self.notebook_workers = {self.hue_sys}

    def run_sql(self, sql, approx_time=10, attempt_times=100, database='default'):
        """
            sql 查询语句
            approx_time 选填，默认查询间隔10秒，尝试时间
            attempt_times  选填，默认查询100次，尝试次数
            database 选填，默认'buffer_fk'
        """
        result = self.beeswax.execute(sql,
                                      database=database,
                                      approx_time=approx_time,
                                      attempt_times=attempt_times)
        return result

    def run_notebook_sql(self,
                         sql: str,
                         database: str = "default",
                         sync=True,
                         new_notebook=False):
        """
            sql 查询语句
            database 选填，默认'default'
            sync 选填，True，是否异步执行sql
        """
        if new_notebook:
            nb = self.hue_sys.new_notebook(self.name,
                                           self.description,
                                           self.hive_settings,
                                           verbose=self.hue_sys.verbose)
        else:
            nb = self.hue_sys

        return nb.execute(sql, database, sync)

    def run_sqls(self, sql_path, workers=3):
        """
            使用多线程，运行多条sql
            sql_path 必填，查询语句存放目录
            approx_time 选填，默认查询间隔10秒，尝试时间
            attempt_times 选填，默认查询100次，尝试次数
            workers 选填，默认值3，同时运行3条sql语句
        """
        th = ThreadPoolExecutor(max_workers=workers)
        result = []
        for root, dirs, files in os.walk(sql_path):
            for file in files:
                # print(file)
                f = open(root + '/' + file)

                with open(root + '/' + file, encoding='utf8') as f:
                    sql = f.read()
                    if re.findall('\-\-', f.readline()):
                        arg = re.sub('\-\-', '', f.readline())

                        arg_dict = json.loads(arg)

                        sql = sql.format(**arg_dict)
                    # print(sql)
                result.append(th.submit(self.run_sql, sql).result())
        return result

    def run_notebook_sqls(self, sqls, database="default", n_jobs=3, ):
        while len(self.notebook_workers) < n_jobs:
            self.notebook_workers.add(self.hue_sys.new_notebook(self.name,
                                                                self.description,
                                                                hive_settings=None,
                                                                verbose=self.hue_sys.verbose))
        d_future = {}
        lst_result = [None] * len(sqls)
        i = 0
        while i < len(sqls) or len(d_future) > 0:
            for notebook, idx in list(d_future.items()):
                try:
                    if notebook._result.is_ready:
                        lst_result[idx] = notebook._result
                        del d_future[notebook]
                except Exception as e:
                    self.hue_sys.log.warning(e)
                    self.hue_sys.log.warning(
                        f"due to fetch_result exception above, "
                        f"result of the following sql is truncated: "
                        f"{sqls[idx][: MAX_LEN_PRINT_SQL] + '...' if len(sqls[idx]) > MAX_LEN_PRINT_SQL else sqls[idx]}")

            while i < len(sqls) and len(d_future) < n_jobs:
                for worker in self.notebook_workers:
                    if i >= len(sqls) or len(d_future) >= n_jobs:
                        break

                    if worker in d_future:
                        continue
                    try:
                        worker.execute(sqls[i],
                                       database=database,
                                       sync=False)
                        d_future[worker] = i
                    except Exception as e:
                        self.hue_sys.log.warning(e)
                        self.hue_sys.log.warning(
                            f"due to execute exception above, "
                            f"result of the following sql is truncated: "
                            f"{sqls[i][: MAX_LEN_PRINT_SQL] + '...' if len(sqls[i]) > MAX_LEN_PRINT_SQL else sqls[i]}")
                    finally:
                        i += 1

        return lst_result

    def split_table(self, table_name, table_size=None, unit_rows=100000):
        th = ThreadPoolExecutor(max_workers=3)
        if table_size is None:
            table_m = table_name.split('.')
            table_size = self.beeswax.table_detail(table_m[1], table_m[0])['details']['stats']['numRows']
        num_sql = '''drop table if exists {table_name}_number;
                create table {table_name}_number as
                select a.*,row_number() over(order by 1) cnt from {table_name} a
            '''.format(table_name=table_name)
        try:
            result = self.run_sql(num_sql)
        except Exception as e:
            print(e)
            print(num_sql)

        assert result == 'succeed'
        table_ns = []
        base_sql = '''drop table if exists {table_name}_{num};
                create table {table_name}_{num} as
                select * from {table_name}_number a
                where cnt >= {start_num}
                        and cnt <= {end_num}
            '''
        results = []
        for i in range(0, int(table_size / unit_rows) + 1):
            start_num = str(i * unit_rows)
            end_num = str((i + 1) * unit_rows - 1)
            print([start_num, end_num])

            try:
                temp_sql = base_sql.format(table_name=table_name, num=str(i), start_num=start_num, end_num=end_num)

                results.append(th.submit(self.run_sql, temp_sql))
            except Exception as e:
                print(e)
                print(temp_sql)
            table_ns.append(table_name + '_' + str(i))

        results[-1].result()
        self.run_sql('drop table %s_number;' % (table_name))
        print('split_down')

        return table_ns

    def download_data(self, table_name, reason, col_info=' ', limit=None, columns=None, Decode_col=[]):
        """
            table_name 下载的表名称
            reason  下载原因，请填写真实原因
            col_info 选填，文本格式，下载表格的列名，逗号隔开，填写后将会在第一行加入列名。
            limit 选填，下载几行数据，不填写则全部下载。
            columns 选填，需要下载的列，不填则全部下载。
            Decode_col 选填，list格式，需要解密的列，不填则不解密。
        """
        table_m = table_name.split('.')
        table_size = int(self.beeswax.table_detail(table_m[1], table_m[0])['details']['stats']['numRows'])

        if table_size > 100000:
            th = ThreadPoolExecutor(max_workers=3)
            tables = self.split_table(table_name, table_size=table_size)
            print(tables)
            results = []
            for table in tables:
                results.append(th.submit(self.download.download_data, table, reason, col_info=col_info, columns=columns,
                                         Decode_col=Decode_col))
            cnt = 1
            for result in results:
                temp_df = result.result()
                if cnt == 1:
                    result_df = temp_df
                    cnt += 1
                else:
                    result_df = result_df.append(temp_df)
            return result_df

        result_df = self.download.download_data(table_name, reason, col_info=col_info, limit=limit, columns=columns,
                                                Decode_col=Decode_col)
        return result_df

    def upload_data(self, file_path, reason, uploadColumnsInfo='1', uploadEncryptColumns='', table_name=None):
        """
            file_path  必填，需要上传文件位置
            reason 必填，上传事由
            uploadColumnsInfo 选填，默认写1，可用作备注，与上传数据无关
            uploadEncryptColumns 选填，默认'',需要加密的列，多个用逗号隔开
            table_name 选填，默认Nnoe，使用自动分配的表名
        """
        uploaded_table = self.download.upload_data(file_path, reason, uploadColumnsInfo=uploadColumnsInfo,
                                                   uploadEncryptColumns=uploadEncryptColumns)
        if table_name != None:
            try:
                self.run_sqls('ALTER TABLE %s RENAME TO %s' % (uploaded_table, table_name))
                print('file uploaded to the table ' + table_name)
                return table_name
            except Exception as e:
                print(e)
                return uploaded_table
        else:
            print('file has uploaded to table ' + uploaded_table)
            return uploaded_table

    def insert_data(self, file_path, table_name, reason, uploadColumnsInfo='1', uploadEncryptColumns=''):
        """
            file_path  必填，需要上传文件位置
            table_name 必填，需要插入数据的表名
            reason 必填，上传事由
            uploadColumnsInfo 选填，默认写1，可用作备注，与上传数据无关
            uploadEncryptColumns 选填，默认'',需要加密的列，多个用逗号隔开
        """
        uploaded_table = self.download.upload_data(self, table_name, reason, uploadColumnsInfo=uploadColumnsInfo,
                                                   uploadEncryptColumns=uploadEncryptColumns)

        try:
            self.run_sqls('insert into table %s \nselect * from %s' % (table_name, uploaded_table))
            print('success')
        except Exception as e:
            print('upload failed, the data is uploaded to the table ' + uploaded_table)
            return uploaded_table

    def reduce_mem_usage(self, df):
        """
            通过调整数据类型，帮助我们减少数据在内存中占用的空间
        """
        #    start_mem = df.memory_usage().sum() # 初始内存分配
        #    print('Memory usage of dataframe is {:.2f} MB'.format(start_mem))

        for col in df.columns:  # 针对每一列
            col_type = df[col].dtype  # 每一列的数据类型
            if re.findall('float|int', str(col_type)):  # 如果不是object类型的
                c_min = df[col].min()  # 这一列的最小值
                c_max = df[col].max()  # 这一列的最大值

                if str(col_type)[:3] == 'int':  # 如果是int类型的
                    # iinfo(type):整数类型的机器限制
                    # iinfo(np.int8)-->iinfo(min=-128, max=127, dtype=int8)
                    # iinfo(np.int16)-->iinfo(min=-32768, max=32767, dtype=int16)
                    # iinfo(np.int32)-->iinfo(min=-2147483648, max=2147483647, dtype=int32)
                    # iinfo(np.int64)-->iinfo(min=-9223372036854775808, max=9223372036854775807, dtype=int64)
                    # 若c_min大于-128 且c_max小于127，就转换为np.int8类型
                    if c_min > np.iinfo(np.int8).min and c_max < np.iinfo(np.int8).max:
                        df[col] = df[col].astype(np.int8)
                    elif c_min > np.iinfo(np.int16).min and c_max < np.iinfo(np.int16).max:
                        df[col] = df[col].astype(np.int16)
                    elif c_min > np.iinfo(np.int32).min and c_max < np.iinfo(np.int32).max:
                        df[col] = df[col].astype(np.int32)
                    elif c_min > np.iinfo(np.int64).min and c_max < np.iinfo(np.int64).max:
                        df[col] = df[col].astype(np.int64)
                else:
                    # finfo(dtype):浮点类型的机器限制
                    if c_min > np.finfo(np.float16).min and c_max < np.finfo(np.float16).max:
                        df[col] = df[col].astype(np.float16)
                    elif c_min > np.finfo(np.float32).min and c_max < np.finfo(np.float32).max:
                        df[col] = df[col].astype(np.float32)
                    else:
                        df[col] = df[col].astype(np.float64)
            else:
                continue
        end_mem = df.memory_usage().sum()
        #    print('Memory usage after optimization is: {:.2f} MB'.format(end_mem)) # 转化后占用内存
        #    print('Decreased by {:.1f}%'.format(100 * (start_mem - end_mem) / start_mem)) # 减少的内存
        return df

    def get_data(self, sql, reason, table_name=None, approx_time=10, attempt_times=100, Decode_col=[], col=' '):
        """
            sql 表格名称使用{cr_text} 代替
            reason 下载原因，请填写真实原因
            approx_time 选填，默认查询间隔10秒，尝试时间
            attempt_times 选填，默认查询100次，尝试次数
        """
        header = 'drop table {cr_text};\ncreate table {cr_text} as                        \n'
        if table_name == None:
            table_name = 'buffer_fk.temp_01'
        try:
            header = header.format(cr_text=table_name)
            sql = header + sql
            print(sql[:500])
            self.run_sql(sql, approx_time, attempt_times)

            result_df = self.download_data(table_name, reason, col_info=col, Decode_col=Decode_col)
            self.reduce_mem_usage(result_df)
            return result_df
        except Exception as e:
            print(e)

    def table_detail(self, table_name, database):
        return self.beeswax.table_detail(table_name, database)

    def close(self):
        for worker in self.notebook_workers:
            worker.close()

        self.hue_sys.close()
        self.hue_sys.logout()
