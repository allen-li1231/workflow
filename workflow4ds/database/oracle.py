import cx_Oracle
from cx_Oracle import InterfaceError
from tqdm.auto import tqdm
import logging
from .. import logger
from ..settings import JUMP_SERVER_ORACLE_HOST, JUMP_SERVER_ORACLE_SERVICE_NAME, JUMP_SERVER_ORACLE_SID, MAX_LEN_PRINT_SQL


class Oracle:
    def __init__(self, username, password,
                 service_name=None, sid=None, hostname=None,
                 port=1521, verbose=False):

        self.verbose = verbose
        self.log = logging.getLogger(__name__ + ".Oracle")
        if self.verbose:
            logger.set_stream_log_level(self.log, verbose=self.verbose)

        """ Connect to the database. """
        self.hostname = hostname or JUMP_SERVER_ORACLE_HOST \
                        or ValueError("hostname not provided in argument or in settings")
        self.port = port
        self.service_name = service_name or JUMP_SERVER_ORACLE_SERVICE_NAME
        self.sid = sid or JUMP_SERVER_ORACLE_SID
        if self.service_name is None and self.sid is None:
            raise ValueError("either service_name or sid should be provided")

        if self.sid is not None:
            self.log.info(f"connect Oracle server for [{username}] on {self.sid}")
            dsn = cx_Oracle.makedsn(self.hostname, str(self.port), self.sid)
        else:
            self.log.info(f"connect Oracle server for [{username}] on {self.service_name}")
            dsn = cx_Oracle.makedsn(self.hostname, str(self.port), service_name=self.service_name)
        try:
            self.db = cx_Oracle.connect(username, password, dsn=dsn)
        except cx_Oracle.DatabaseError as e:
            self.log.exception(e)
            raise e
        # If the database connection succeeded create the cursor
        # we-re going to use.
        self.db.autocommit = True
        self.cursor = self.db.cursor()

    def close(self):
        """
        Disconnect from the database. If this fails, for instance
        if the connection instance doesn't exist, ignore the exception.
        """
        try:
            self.cursor.close()
            self.db.close()
        except cx_Oracle.DatabaseError as e:
            self.log.exception(e)
            pass

    def execute(self, sql):
        """
        Execute whatever SQL statements are passed to the method;
        commit if specified. Do not specify fetchall() in here as
        the SQL statement may not be a select.
        """
        try:
            self.log.info(f"execute sql: {sql[:MAX_LEN_PRINT_SQL]}")
            self.cursor.execute(sql.strip(";\n\b\t"))
        except cx_Oracle.DatabaseError as e:
            # Log error as appropriate
            self.log.exception(e)
            raise e

    def execute_proc(self, sql):
        """
        Execute whatever SQL procedure are passed to the method;
        commit if specified.
        """
        try:
            self.log.info(f"execute procedure: {sql[:MAX_LEN_PRINT_SQL]}")
            self.cursor.callproc(sql)
        except cx_Oracle.DatabaseError as e:
            # Log error as appropriate
            self.log.exception(e)
            raise e

    def fetchall(self):
        data = self.cursor.fetchall()
        col_names = []
        for i in range(0, len(self.cursor.description)):
            col_names.append(self.cursor.description[i][0])

        return {"data": data, "columns": col_names}

    def fetchmany(self, n_rows):
        data = self.cursor.fetchmany(n_rows)
        col_names = []
        for i in range(0, len(self.cursor.description)):
            col_names.append(self.cursor.description[i][0])

        return {"data": data, "columns": col_names}

    def run_sql(self, sql: str, n_rows: int = -1, return_df=True):
        self.execute(sql)
        try:
            if n_rows <= 0:
                data = self.fetchall()
            else:
                data = self.fetchmany(n_rows)
        except InterfaceError as e:
            if str(e) == "not a query":
                return
            raise e

        if return_df:
            import pandas as pd
            return pd.DataFrame(**data)
        return data

    def desc(self, owner: str, table_name: str, upper_case=True, return_df=True):
        if upper_case:
            owner = owner.upper()
            table_name = table_name.upper()
    
        self.cursor.execute(f"""
            select
                tab.column_name,
                cmt.comments,
                tab.data_type,
                tab.data_length,
                tab.data_precision,
                tab.NUM_DISTINCT,
                tab.NUM_NULLS,
                tab.density
            from all_tab_columns tab
            join all_col_comments cmt
                on tab.table_name = cmt.table_name
                and tab.owner = cmt.owner
                and tab.column_name = cmt.column_name
            where tab.table_name = '{table_name}' and tab.owner = '{owner}'""")

        data = self.fetchall()
        if return_df:
            import pandas as pd
            return pd.DataFrame(**data)
        return data

    def insert_into(self, table_name: str, dataframe, chunksize=1024):
        for i in tqdm(range(0, dataframe.shape[0], chunksize)):
            s_values = ''.join([f'\ninto {table_name} values ('
                                + ','.join([f"'{val}'"
                                            if isinstance(val, (str, int, float))
                                            else f"TO_DATE('{val.strftime("%Y-%m-%d %H:%M:%S")}', 'yyyy-mm-dd hh24:mi:ss')"
                                            for val in row])
                                + ')' for row in dataframe.iloc[i: i+chunksize-1, :].values
            ])
            sql_load = f"insert all {s_values}\nSELECT * FROM DUAL;"
            self.execute(sql_load)

        return dataframe.shape[0]
