import rethinkdb
from rethinkdb.ast import DB

from regrid.constants import \
    STARTED_DATE_JSON_NAME, \
    FINISHED_DATE_JSON_NAME, \
    FILE_NAME_JSON_NAME, \
    STATUS_JSON_NAME, \
    CHUNK_SIZE_BYTES_JSON_NAME, \
    FILE_ID_JSON_NAME, \
    NUM_JSON_NAME, \
    DEFAULT_CHUNK_SIZE
from regrid.stream import UploadStream


class Bucket:
    def __init__(self, database_name, bucket_name="fs", files_table_name="files",
                 file_index="file_index", file_prefix_index="prefix_index", chunks_table_name="chunks",
                 chunk_index="chunk_index", chunk_size=DEFAULT_CHUNK_SIZE, table_create_options=None):
        self.db_query = rethinkdb.db(database_name)
        self.mounted = False

        self.bucket_name = bucket_name
        self.files_table_name = f"{self.bucket_name}_{files_table_name}"
        self.file_index = file_index
        self.file_prefix_index = file_prefix_index
        self.chunks_table_name = f"{self.bucket_name}_{chunks_table_name}"
        self.chunk_index = chunk_index
        self.chunk_size = chunk_size
        self.table_create_options = table_create_options

    def mount(self):
        if self.mounted:
            return

        query = self._ensure_table_query(self.db_query, self.files_table_name)
        file_index_func = lambda row: rethinkdb.args(
            [row[STATUS_JSON_NAME], row[FILE_NAME_JSON_NAME], row[FINISHED_DATE_JSON_NAME]])
        file_prefix_index_func = lambda row: rethinkdb.expr(row[STATUS_JSON_NAME] == "completed").branch(
            rethinkdb.args([row[FILE_NAME_JSON_NAME].split("/").slice(1, -1), row[FINISHED_DATE_JSON_NAME]]),
            rethinkdb.error("File is still uploading.")
        )
        query = query.do(lambda result: rethinkdb.expr(result["tables_created"] == 1).branch(
            self._create_index(self.db_query, self.files_table_name, self.file_index, file_index_func)
                .do(lambda _:
                    self._create_index(self.db_query, self.files_table_name, self.file_prefix_index,
                                       file_prefix_index_func)),
            None
        ))

        query = query.do(lambda _: self._ensure_table_query(self.db_query, self.chunks_table_name))
        chunk_index_func = lambda row: rethinkdb.args([row[FILE_ID_JSON_NAME], row[NUM_JSON_NAME]])
        query = query.do(lambda result: rethinkdb.expr(result["tables_created"] == 1).branch(
            self._create_index(self.db_query, self.chunks_table_name, self.chunk_index, chunk_index_func),
            None
        ))
        query = query.do(lambda _: self._confirm_mount())
        return query

    def _confirm_mount(self):
        self.mounted = True

    @staticmethod
    def _ensure_table_query(db_query: DB, table_name):
        return db_query.table_list() \
            .contains(table_name) \
            .branch(
            {"tables_created": 0},
            db_query.table_create(table_name))

    @staticmethod
    def _create_index(db_query: DB, table_name, index_name, index_func):
        return db_query.table(table_name).index_create(index_name, index_func).do(
            lambda _: db_query.table(table_name).index_wait(index_name))

    def file_uploader(self, file_name, source_buffer, conn, chunk_size=None):
        return self._create_upload_stream(file_name, source_buffer, conn, chunk_size=chunk_size or self.chunk_size)

    def _create_upload_stream(self, file_name, buffer, conn, chunk_size):
        file_document = {
            STATUS_JSON_NAME: "incomplete",
            FILE_NAME_JSON_NAME: file_name,
            STARTED_DATE_JSON_NAME: rethinkdb.now(),
            CHUNK_SIZE_BYTES_JSON_NAME: chunk_size
        }
        query = self.db_query.table(self.files_table_name).insert(file_document)
        file_id = query.run(conn)["generated_keys"][0]
        return UploadStream(self, file_id, file_document, buffer, conn)
