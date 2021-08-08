from enum import Enum
from fastapi import FastAPI
import os
from download_warc_urls import WARC_URLS_PATH
import pandas as pd
import shortuuid
from threading import Thread
import random

app = FastAPI()

# Enum class describing the status of a block
class BlockStatus(int, Enum):
    AVAILABLE = 0
    IN_PROGRESS = 1
    COMPLETED = 2
    FAILED = 3


# if file exists, resume from db
if os.path.isfile("blocks.h5"):
    BLOCKS = pd.read_hdf("blocks.h5", key="df")
else:
    if not os.path.exists(WARC_URLS_PATH):
        print("No warc urls found - run download_warc_urls.py")
        # exit(1)
        BLOCK_URLS = list([f"block_{i}" for i in range(100)])
    else:
        with open(WARC_URLS_PATH, "r") as f:
            BLOCK_URLS = list(f.readlines())
        print(f"{len(BLOCK_URLS)} block urls loaded")
    BLOCKS = pd.DataFrame(
        {
            "url": BLOCK_URLS,
            "uuid": [shortuuid.uuid() for _ in range(len(BLOCK_URLS))],
            "status": [BlockStatus(0) for _ in range(len(BLOCK_URLS))],
            "worker_id": ["N/A" for _ in range(len(BLOCK_URLS))],
        }
    )

print(f"{len(BLOCKS)} blocks loaded")
GLOBAL_PROGRESS = int((BLOCKS.status.values == BlockStatus.COMPLETED).sum())
print(f"global progress = {GLOBAL_PROGRESS}")


def _save_progress():
    try:
        BLOCKS.to_hdf("blocks.h5", key="df", mode="w")
    except ValueError:
        print(f"save failed - might already be saving in another thread")


def save_progress():
    Thread(target=_save_progress).start()


def get_idx_by_id(block_id):
    return BLOCKS.index[BLOCKS["uuid"] == block_id].tolist()[0]


# get an available block
@app.get("/blocks/get")
async def get_block():
    # get first index where status is AVAILABLE or FAILED
    cond = (BLOCKS.status == BlockStatus.AVAILABLE) | (
        BLOCKS.status == BlockStatus.FAILED
    )
    available = BLOCKS.index[cond].tolist()
    n_available = len(available)
    if n_available > 0:
        # pick a random index
        idx = random.randint(0, n_available - 1)
        url = BLOCKS.loc[idx, "url"]
        uuid = BLOCKS.loc[idx, "uuid"]
        return {"url": url, "uuid": uuid}
    # if no blocks are available, return an error
    return {"message": "No blocks available"}


# get total number of blocks
@app.get("/blocks/count")
async def get_block_count():
    return {"count": len(BLOCKS)}


# get global progress
@app.get("/blocks/progress")
async def get_progress():
    print(GLOBAL_PROGRESS, type(GLOBAL_PROGRESS))
    return {"progress": GLOBAL_PROGRESS}


# mark a block as in progress
@app.put("/blocks/in_progress/{block_id}")
async def mark_block_in_progress(block_id: str, worker_id: str):
    try:
        idx = get_idx_by_id(block_id)
    except:
        return {"message": f"Block {block_id} not found"}
    BLOCKS.at[idx, "status"] = BlockStatus.IN_PROGRESS
    BLOCKS.at[idx, "worker_id"] = worker_id
    return {"message": "success"}


# mark a block as completed
@app.put("/blocks/complete/{block_id}")
async def mark_block_completed(block_id: str):
    global GLOBAL_PROGRESS
    try:
        idx = get_idx_by_id(block_id)
    except:
        return {"message": f"Block {block_id} not found"}
    BLOCKS.at[idx, "status"] = BlockStatus.COMPLETED
    # save out blocks to disk
    save_progress()
    GLOBAL_PROGRESS += 1
    return {"message": "success"}


# mark a block as failed
@app.put("/blocks/failed/{block_id}")
async def mark_block_failed(block_id: str):
    try:
        idx = get_idx_by_id(block_id)
    except:
        return {"message": f"Block {block_id} not found"}
    BLOCKS.at[idx, "status"] = BlockStatus.COMPLETED
    return {"message": "success"}

