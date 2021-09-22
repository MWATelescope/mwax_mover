import psycopg2
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

    def insert_one_row(self, sql: str, parm_list: list) -> int:
        if self.dummy:
            return 1
        else:
            return self.insert_one_row_postgres(sql, parm_list)

    def insert_one_row_postgres(self, sql: str, parm_list: list) -> int:
        cursor = None

        con = psycopg2.connect(host=self.host,
                               database=self.db,
                               user=self.user,
                               password=self.password)
        try:
            cursor = con.cursor()
            cursor.execute(sql, parm_list)

        except Exception as exception_info:
            if con:
                con.rollback()
            raise exception_info
        else:
            # getting in here means either:
            # a) We inserted the row OK --OR--
            # b) The filename already existed in this table so we updated it
            rows_affected = cursor.rowcount
            print(f"Rows affected {rows_affected}")

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
                con.close()


def upsert_data_file_row(logger,
                         db_handler_object,
                         archive_filename: str,
                         filetype: int,
                         hostname: str,
                         remote_archived: bool,
                         location,
                         prefix) -> bool:
    # Prepare the fields
    # immediately add this file to the db so we insert a record into metadata data_files table
    filename = os.path.basename(archive_filename)
    obsid = int(filename[0:10])
    file_size = os.stat(archive_filename).st_size
    deleted = False

    # We actually do an upsert- this way we can use the same code for mwax (insert) and mwacache (update)
    sql = f"INSERT INTO data_files " \
          f"(observation_num, filetype, size, filename, host, remote_archived, deleted, location, prefix) " \
          f"VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (filename) DO UPDATE SET " \
          f"remote_archived = excluded.remote_archived, location = excluded.location, prefix = excluded.prefix"

    try:
        rows_inserted = db_handler_object.insert_one_row(sql, (str(obsid), filetype, file_size,
                                                         filename, hostname,
                                                         remote_archived, deleted, location, prefix))

        if db_handler_object.dummy:
            logger.warning(f"{filename} upsert_data_file_row() Using dummy database connection. "
                           f"No data is really being upserted")
        else:
            if rows_inserted == 1:
                logger.info(f"{filename} upsert_data_file_row() Successfully write into data_files table")

        return_value = True
    except Exception as insert_exception:
        logger.error(f"{filename} insert_data_file_row() inserting data_files record in "
                     f"data_files table: {insert_exception}. SQL was {sql}")
        return_value = False

    return return_value
