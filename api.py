import requests
import os
import shortuuid

BASE = "http://127.0.0.1:8000"
GET_BLOCK = f"{BASE}/blocks/get"
GET_BLOCK_COUNT = f"{BASE}/blocks/count"
GLOBAL_PROGRESS = f"{BASE}/blocks/progress"
BLOCK_IN_PROGRESS = f"{BASE}/blocks/in_progress"
BLOCK_COMPLETE = f"{BASE}/blocks/complete"
BLOCK_FAILED = f"{BASE}/blocks/failed"
WORKER_ID = None


def worker_id():
    global WORKER_ID
    if WORKER_ID is None:
        # set worker id
        WORKER_ID = f"{os.getlogin()}-{shortuuid.uuid()}"
    return WORKER_ID


def get_available_block():
    response = requests.get(GET_BLOCK)
    return response.json()


def get_block_count():
    response = requests.get(GET_BLOCK_COUNT)
    return response.json().get("count", response.json())


def get_global_progress():
    response = requests.get(GLOBAL_PROGRESS)
    return response.json().get("progress", response.json())


def mark_block_in_progress(block_id):
    response = requests.put(f"{BLOCK_IN_PROGRESS}/{block_id}?worker_id={worker_id()}")
    return response.json()


def mark_block_complete(block_id):
    response = requests.put(f"{BLOCK_COMPLETE}/{block_id}")
    return response.json()


def mark_block_failed(block_id):
    response = requests.put(f"{BLOCK_FAILED}/{block_id}")
    return response.json()


def test():
    for _ in range(100):
        print(_)
        url, uuid = get_available_block().values()
        mark_block_complete(uuid)


if __name__ == "__main__":
    test()
