import sqlite3
import pandas as pd
import random
from functools import wraps
from time import time, sleep
import os
from enum import Enum
import shortuuid
from tqdm import tqdm

# Enum class describing the status of a block
class BlockStatus(int, Enum):
    AVAILABLE = 0
    IN_PROGRESS = 1
    COMPLETED = 2
    FAILED = 3


def timer(func):
    @wraps(func)
    def _time_it(*args, **kwargs):
        start = time()
        try:
            return func(*args, **kwargs)
        finally:
            end_ = time() - start
            print(f"Total execution time of {func.__name__}: {end_} ms")

    return _time_it


class DB:
    def __init__(
        self, path="blocks.sql", commit_interval=1, warc_urls_path="warc_urls.txt"
    ):
        self.path = path
        self.warc_urls_path = warc_urls_path
        self.con = sqlite3.connect(self.path)
        self.create_db()
        self.commit_interval = commit_interval
        self.counter = 0

    def _table_exists(self, table):
        cur = self.con.cursor()
        cur.execute(
            f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table}'"
        )
        return bool(cur.fetchone())

    def now(self):
        return pd.Timestamp.now().value // 10 ** 9

    def __len__(self):
        return self.get_n_rows()

    @timer
    def create_db(self):
        # create db if not exists
        if not self._table_exists("blocks"):
            if not os.path.exists(self.warc_urls_path):
                print("No warc urls found - run download_warc_urls.py")
                exit(1)
            else:
                with open(self.warc_urls_path, "r") as f:
                    BLOCK_URLS = list(tqdm(f.readlines(), desc="Reading WARC urls"))
                print(f"{len(BLOCK_URLS)} block urls loaded")
            now = self.now()
            BLOCKS = pd.DataFrame(
                {
                    "url": BLOCK_URLS,
                    "uuid": [shortuuid.uuid() for _ in range(len(BLOCK_URLS))],
                    "status": [BlockStatus(0) for _ in range(len(BLOCK_URLS))],
                    "worker_id": ["N/A" for _ in range(len(BLOCK_URLS))],
                    "last_updated": [now for _ in range(len(BLOCK_URLS))],
                }
            )
            # save to db
            print("CREATED TABLE OF SIZE : ", len(BLOCKS))
            BLOCKS.to_sql("blocks", self.con, if_exists="replace", index=False)

    @timer
    def update_status(self, uuid, status, worker_id=None, commit=False):
        """
        Updates the status of the row with uuid `uuid` to `status`
        """
        cur = self.con.cursor()
        worker_id_str = "" if worker_id is None else f", worker_id={worker_id}"
        cur.execute(
            f"UPDATE blocks SET status = {status}, last_updated = {self.now()}{worker_id_str} WHERE uuid = '{uuid}'"
        )
        self.counter += 1
        if (self.counter % self.commit_interval == 0) or commit:
            self.con.commit()

    @timer
    def get_status(self, uuid):
        """
        Gets the status of the row where uuid==`uuid`
        """
        cur = self.con.cursor()
        cur.execute(f"SELECT status FROM blocks WHERE uuid = {uuid}")
        return cur.fetchone()[0]

    @timer
    def get_available_block(self, to_fetch=100):
        """
        Gets a single block where status is available (or failed, since it needs to be retried)
        """
        cur = self.con.cursor()
        # available is where status is 0 (AVAILABLE) or 3 (FAILED)
        # cur.execute(
        #     f"SELECT url, uuid, last_updated FROM blocks WHERE status IN (0, 3)"
        # )
        cur.execute(f"SELECT url, uuid, last_updated FROM blocks WHERE status IN (2)")
        # select many then pick one to avoid overlaps between threads
        candidates = cur.fetchmany(to_fetch)
        url, uuid, time = random.choice(candidates)  # time = time last updated
        # update status to 1 (IN_PROGRESS)
        self.update_status(uuid, 1)
        return url, uuid, time

    @timer
    def get_blocks_with_status(self, status):
        """
        Gets all blocks where status==`status`
        """
        cur = self.con.cursor()
        cur.execute(f"SELECT url, uuid, status FROM blocks WHERE status = {status}")
        return cur.fetchall()

    @timer
    def get_n_rows(self):
        """
        Gets the number of rows in the table (i.e total number of blocks)
        """
        cur = self.con.cursor()
        cur.execute("SELECT COUNT(*) FROM blocks")
        return cur.fetchone()[0]

    @timer
    def get_progress(self):
        """
        Gets the number of completed blocks
        """
        # completed is where status is 2 (COMPLETED)
        cur = self.con.cursor()
        cur.execute(f"SELECT COUNT(*) FROM blocks WHERE status = 2")
        return cur.fetchone()[0]
