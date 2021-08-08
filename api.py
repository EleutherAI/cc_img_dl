import requests
import os
import shortuuid


class API:
    def __init__(self, host="http://127.0.0.1", port="5000"):
        self.host = host
        self.port = port
        BASE = f"{self.host}:{self.port}"
        self.GET_BLOCK = f"{BASE}/blocks/get"
        self.GET_BLOCK_COUNT = f"{BASE}/blocks/count"
        self.GLOBAL_PROGRESS = f"{BASE}/blocks/progress"
        self.BLOCK_IN_PROGRESS = f"{BASE}/blocks/in_progress"
        self.BLOCK_COMPLETE = f"{BASE}/blocks/complete"
        self.BLOCK_FAILED = f"{BASE}/blocks/failed"
        self.WORKER_ID = None

    def worker_id(self):
        if self.WORKER_ID is None:
            # set worker id
            self.WORKER_ID = f"{os.getlogin()}-{shortuuid.uuid()}"
        return self.WORKER_ID

    def get_available_block(self):
        response = requests.get(f"{self.GET_BLOCK}/?worker_id={self.worker_id()}")
        return response.json()

    def get_block_count(self):
        response = requests.get(self.GET_BLOCK_COUNT)
        return response.json().get("count", response.json())

    def get_global_progress(self):
        response = requests.get(self.GLOBAL_PROGRESS)
        return response.json().get("progress", response.json())

    def mark_block_in_progress(self, block_id):
        response = requests.put(
            f"{self.BLOCK_IN_PROGRESS}/{block_id}?worker_id={self.worker_id()}"
        )
        return response.json()

    def mark_block_complete(self, block_id):
        response = requests.put(f"{self.BLOCK_COMPLETE}/{block_id}")
        return response.json()

    def mark_block_failed(self, block_id):
        response = requests.put(f"{self.BLOCK_FAILED}/{block_id}")
        return response.json()


def test():
    api = API()
    for _ in range(100):
        print(_)
        url, uuid = api.get_available_block().values()
        api.mark_block_complete(uuid)


if __name__ == "__main__":
    test()