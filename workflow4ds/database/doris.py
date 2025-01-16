from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
from sqlalchemy.exc import DatabaseError, ResourceClosedError
from sqlalchemy.sql.expression import text

import pandas as pd
import logging

from .. import logger
from ..settings import MAX_LEN_PRINT_SQL

class Doris:
    def __init__(self, username: str, password: str,
                 hostname: str, port=9030,
                 database=None, verbose=False):

        self.verbose = verbose
        self.log = logging.getLogger(__name__ + ".Doris")
        if self.verbose:
            logger.set_stream_log_level(self.log, verbose=self.verbose)

        """ Connect to the database. """
        self.hostname = hostname
        self.port = port
        self.username = username

        uri = URL.create(
            drivername="doris",
            username=username,
            password=password,
            host=hostname,
            port=port,
            database=database
        )
        self.engine = create_engine(uri)
        self.db = self.engine.connect()

    def close(self):
        """
        Disconnect from the database. If this fails, for instance
        if the connection instance doesn't exist, ignore the exception.
        """
        try:
            self.db.close()
        except DatabaseError as e:
            self.log.exception(e)
            pass

    def run_sql(self, sql: str, n_rows=-1, return_df=True):
        """
        Execute whatever SQL statements are passed to the method;
        commit if specified. Do not specify fetchall() in here as
        the SQL statement may not be a select.
        """
        try:
            self.log.info(f"execute sql: {sql[:MAX_LEN_PRINT_SQL]}")
            res = self.db.execute(text(sql.strip("\n\b\t")))
        except DatabaseError as e:
            # Log error as appropriate
            self.log.exception(e)
            raise e

        try:
            if n_rows < 0:
                data = res.fetchall()
            else:
                data = res.fetchmany(n_rows)
        except ResourceClosedError as e:
            if str(e) == "This result object does not return rows. It has been closed automatically.":
                return
            self.log.exception(e)
            raise e

        if return_df:
            return pd.DataFrame(data, columns=res.keys())

        return data

    def show_create_table(self, table_name):
        res = self.db.execute(text(f"show create table {table_name}")).fetchall()
        print(res[0][1])