import adbc_driver_manager
import adbc_driver_flightsql.dbapi as flight_sql

import pandas as pd
import logging
from .. import logger


class Doris:
    def __init__(self, username: str, password: str,
                 hostname: str, port=9030, verbose=False):

        self.verbose = verbose
        self.log = logging.getLogger(__name__ + ".Doris")
        if self.verbose:
            logger.set_stream_log_level(self.log, verbose=self.verbose)

        """ Connect to the database. """
        self.hostname = hostname
        self.port = port
        self.username = username

        uri = f"grpc://{hostname}:{port}"
        self.db = flight_sql.connect(uri=uri, db_kwargs={
            adbc_driver_manager.DatabaseOptions.USERNAME.value: username,
            adbc_driver_manager.DatabaseOptions.PASSWORD.value: password,
        })
        # If the database connection succeeded create the cursor
        # we-re going to use.
        self.cursor = self.db.cursor()

    def close(self):
        """
        Disconnect from the database. If this fails, for instance
        if the connection instance doesn't exist, ignore the exception.
        """
        try:
            self.cursor.close()
            self.db.close()
        except adbc_driver_manager.DatabaseError as e:
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
            self.cursor.execute(sql.strip("\n\b\t"))
        except adbc_driver_manager.DatabaseError as e:
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
        except adbc_driver_manager.DatabaseError as e:
            # Log error as appropriate
            self.log.exception(e)
            raise e

    def fetchall(self):
        data = self.cursor.fetch_arrow_table()
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
        if n_rows <= 0:
            if return_df:
                return self.cursor.fetch_df()

            return self.cursor.fetchall()
        else:
            data = self.fetchmany(n_rows)
            if return_df:
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
