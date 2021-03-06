import psycopg2
import psycopg2.pool
import os

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


def insert_data_file_row(logger,
                         db_handler_object,
                         archive_filename: str,
                         filetype: int,
                         hostname: str) -> bool:
    # Prepare the fields
    # immediately add this file to the db so we insert a record into metadata data_files table
    filename = os.path.basename(archive_filename)
    obsid = int(filename[0:10])
    file_size = os.stat(archive_filename).st_size
    remote_archived = False  # This gets set to True by NGAS at Pawsey
    deleted = False
    site_path = f"http://mwangas/RETRIEVE?file_id={obsid}"

    sql = f"INSERT INTO data_files " \
          f"(observation_num, filetype, size, filename, site_path, host, remote_archived, deleted) " \
          f"VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (filename) DO NOTHING"

    try:
        rows_inserted = db_handler_object.insert_one_row(sql, (str(obsid), filetype, file_size,
                                                         filename, site_path, hostname,
                                                         remote_archived, deleted))

        if db_handler_object.dummy:
            logger.warning(f"{filename} insert_data_file_row() Using dummy database connection. "
                           f"No data is really being inserted")
        else:
            if rows_inserted == 1:
                logger.info(f"{filename} insert_data_file_row() Successfully inserted into data_files table")
            else:
                logger.info(f"{filename} insert_data_file_row() Row already exists in data_files table")

        return_value = True
    except Exception as insert_exception:
        logger.error(f"{filename} insert_data_file_row() inserting data_files record in "
                     f"data_files table: {insert_exception}")
        return_value = False

    return return_value
