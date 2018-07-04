import asyncio

import rethinkdb

from regrid.bucket import Bucket

if __name__ == '__main__':
    with rethinkdb.connect("localhost", 28015) as conn:
        bucket = Bucket("pyregrid")
        print("Mounting...")
        result = bucket.mount().run(conn)
        print("Mounted")

        loop = asyncio.get_event_loop()
        with open("big.gz", mode="rb") as source:
            with bucket.file_uploader("/files/big.gz", source, conn) as uploader:
                loop.run_until_complete(uploader.upload_async())
