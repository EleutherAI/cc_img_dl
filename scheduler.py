from enum import Enum
from typing import Optional
from fastapi import FastAPI
import os
from download_warc_urls import WARC_URLS_PATH
import uvicorn
from db import DB

app = FastAPI()
DATABASE = None
GLOBAL_PROGRESS = 0


# Enum class describing the status of a block
class BlockStatus(int, Enum):
    AVAILABLE = 0
    IN_PROGRESS = 1
    COMPLETED = 2
    FAILED = 3


@app.on_event("startup")
async def startup():
    global DATABASE
    global GLOBAL_PROGRESS
    if not os.path.exists(WARC_URLS_PATH):
        print("No warc urls found - run download_warc_urls.py")
        exit(1)
    DATABASE = DB(path="blocks_2.sql")
    GLOBAL_PROGRESS = DATABASE.get_progress()
    print(f"{len(DATABASE)} blocks loaded")
    print(f"global progress = {GLOBAL_PROGRESS}")


@app.on_event("shutdown")
async def shutdown():
    print("saving progress")
    DATABASE.con.commit()


# get an available block (or N)
@app.get("/blocks/get")
async def get_block(worker_id: str, n: Optional[int] = None):
    # get first index where status is AVAILABLE or FAILED
    try:
        url, uuid, last_updated = DATABASE.get_available_block()
    except IndexError as e:
        print(e)
        return {"message": "no blocks available"}
    return {"url": url, "uuid": uuid, "last_updated": last_updated}


# get total number of blocks
@app.get("/blocks/count")
async def get_block_count():
    return {"count": len(DATABASE)}


# get global progress
@app.get("/blocks/progress")
async def get_progress():
    global GLOBAL_PROGRESS
    GLOBAL_PROGRESS = DATABASE.get_progress()
    print(f"Global Progress: {GLOBAL_PROGRESS}")
    return {"progress": GLOBAL_PROGRESS}


# mark a block as in progress
@app.put("/blocks/in_progress/{block_id}")
async def mark_block_in_progress(block_id: str, worker_id: str):
    DATABASE.update_status(block_id, int(BlockStatus.IN_PROGRESS), worker_id)
    return {"message": "success"}


# mark a block as completed
@app.put("/blocks/complete/{block_id}")
async def mark_block_completed(block_id: str):
    # TODO: what if the block does not exist?
    DATABASE.update_status(block_id, int(BlockStatus.COMPLETED))
    return {"message": "success"}


# mark a block as failed
@app.put("/blocks/failed/{block_id}")
async def mark_block_failed(block_id: str):
    # TODO: what if the block does not exist?
    DATABASE.update_status(block_id, int(BlockStatus.FAILED))
    return {"message": "success"}


if __name__ == "__main__":
    # run uvicorn app
    uvicorn.run(
        "scheduler:app", host="0.0.0.0", port=5000, log_level="info", workers=2,
    )

