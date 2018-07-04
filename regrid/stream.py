import asyncio

import rethinkdb

from regrid.constants import \
    FINISHED_DATE_JSON_NAME, \
    STATUS_JSON_NAME, \
    LENGTH_JSON_NAME, \
    CHUNK_SIZE_BYTES_JSON_NAME, \
    FILE_ID_JSON_NAME, \
    NUM_JSON_NAME, \
    DATA_JSON_NAME


async def _run_async(query, conn):
    return await query.run(conn)


class UploadStream:

    def __init__(self, bucket, file_id, file_document, buffer, conn):
        self.bucket = bucket
        self.file_id = file_id
        self.file_document = file_document
        self.buffer = buffer
        self._conn = conn
        self._chunk_index = 0
        self._file_length = 0
        self._open = False

    def __enter__(self):
        self._open = True
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._open:
            raise Exception("Stream has ended prematurely.")
        self.bucket.db_query.table(self.bucket.files_table_name).get(self.file_id).update(
            {
                STATUS_JSON_NAME: "complete",
                LENGTH_JSON_NAME: self._file_length,
                FINISHED_DATE_JSON_NAME: rethinkdb.now()
            }
        ).run(self._conn)
        return self

    def upload_sync(self):
        if not self._open:
            raise Exception("Stream is closed.")
        while self.chunk().run(self._conn):
            pass
        self._open = False

    def chunk(self, consumer=None, query_ending=True):
        data = self.buffer.read(self.file_document[CHUNK_SIZE_BYTES_JSON_NAME])
        if not data:
            if query_ending:
                return self.bucket.db_query.do(lambda _: None)
            return None
        binary = rethinkdb.binary(data)
        chunk_document = {
            FILE_ID_JSON_NAME: self.file_id,
            NUM_JSON_NAME: self._chunk_index,
            DATA_JSON_NAME: binary
        }
        self._file_length += len(data)
        self._chunk_index += 1
        query = self.bucket.db_query.table(self.bucket.chunks_table_name).insert(chunk_document)
        if consumer is not None:
            consumer(query)
        return query

    @asyncio.coroutine
    def upload_async(self):
        if not self._open:
            raise Exception("Stream is closed.")
        self._open = False
        tasks = []
        while self.chunk(query_ending=False,
                         consumer=lambda query: tasks.append(_run_async(query, self._conn))):
            pass
        yield from asyncio.gather(*tasks)
