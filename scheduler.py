from enum import Enum
from fastapi import FastAPI
import os
from download_warc_urls import WARC_URLS_PATH
import pandas as pd
import shortuuid
from threading import Thread, Lock
import random
import uvicorn
from tables.exceptions import HDF5ExtError
from functools import partial

app = FastAPI()
lock = Lock()
BLOCKS = None
GLOBAL_PROGRESS = 0


# Enum class describing the status of a block
class BlockStatus(int, Enum):
    AVAILABLE = 0
    IN_PROGRESS = 1
    COMPLETED = 2
    FAILED = 3


@app.on_event("startup")
async def startup():
    global BLOCKS
    global GLOBAL_PROGRESS
    # if file exists, resume from db
    if os.path.isfile("blocks.h5"):
        try:
            BLOCKS = pd.read_hdf("blocks.h5", key="df")
        except:
            print("failed to read blocks.h5")
            exit(1)
    else:
        if not os.path.exists(WARC_URLS_PATH):
            print("No warc urls found - run download_warc_urls.py")
            exit(1)
            # for debugging:
            # BLOCK_URLS = list([f"block_{i}" for i in range(300)])
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


def _save_progress(pth="./blocks.h5"):
    with lock:
        try:
            BLOCKS.to_hdf(pth, key="df", mode="w")
        except (HDF5ExtError, ValueError):
            print(f"save failed")


def save_progress(pth="./blocks.h5"):
    fn = partial(_save_progress, pth)
    Thread(target=fn).start()


@app.on_event("shutdown")
async def shutdown():
    print("saving progress")
    save_progress()


def get_idx_by_id(block_id):
    return BLOCKS.index[BLOCKS["uuid"] == block_id].tolist()[0]


# get an available block
@app.get("/blocks/get")
async def get_block(worker_id: str):
    # get first index where status is AVAILABLE or FAILED
    cond = (BLOCKS.status == BlockStatus.AVAILABLE) | (
        BLOCKS.status == BlockStatus.FAILED
    )
    available = BLOCKS.index[cond].tolist()
    n_available = len(available)
    if n_available > 0:
        # pick a random index
        idx = available[random.randint(0, n_available - 1)]
        url = BLOCKS.loc[idx, "url"]
        uuid = BLOCKS.loc[idx, "uuid"]
        # mark block as in progress
        BLOCKS.at[idx, "status"] = BlockStatus.IN_PROGRESS
        BLOCKS.at[idx, "worker_id"] = worker_id
        # return data
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
    print(f"Global Progress: {GLOBAL_PROGRESS}")
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
    GLOBAL_PROGRESS += 1
    # save out blocks to disk every so often
    if GLOBAL_PROGRESS % 100 == 0:
        save_progress()
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


if __name__ == "__main__":
    # run uvicorn app
    uvicorn.run("scheduler:app", host="0.0.0.0", port=5000, log_level="info")

