import psycopg2
import psycopg2.pool

DUMMY_DB = "dummy"


class MWAXDBHandler:
    def __init__(self, host: str, port: int, db, user: str, password: str):
        self.host = host
        self.port = port
        self.db = db
        self.user = user
        self.password = password
        self.dummy = self.host == DUMMY_DB

        if self.dummy:
            #
            # This is a mock db provider for when we are testing
            #
            self.db_pool = None
        else:
            #
            # This is the real db pool provider for production
            #
            self.db_pool = psycopg2.pool.ThreadedConnectionPool(minconn=1,
                                                                maxconn=8,
                                                                host=self.host,
                                                                database=self.db,
                                                                user=self.user,
                                                                password=self.password,
                                                                port=self.port)

    def insert_one_row(self, sql: str, parm_list: list) -> int:
        if self.dummy:
            return 1
        else:
            self.insert_one_row_postgres(sql, parm_list)

    def insert_one_row_postgres(self, sql: str, parm_list: list) -> int:
        cursor = None
        con = None

        try:
            con = self.db_pool.getconn()
            cursor = con.cursor()
            cursor.execute(sql, parm_list)

        except Exception as exception_info:
            if con:
                con.rollback()
            raise exception_info
        else:
            # getting in here means either:
            # a) We inserted the row OK --OR--
            # b) The filename already existed in this table, so ignore (maybe we're reprocessing this file?)
            rows_affected = cursor.rowcount

            if rows_affected == 1:
                if con:
                    con.commit()
            else:
                if con:
                    con.rollback()

            return rows_affected
        finally:
            if cursor:
                cursor.close()
            if con:
                self.db_pool.putconn(conn=con)
