import subprocess
import multiprocessing
from multiprocessing import set_start_method, Value
from functools import partial
import api
import pathlib
import time
import argparse
import traceback

COUNTER = Value("i", 0)


def process_wats(output_path, debug=False):
    global COUNTER
    while True:
        print(
            f"\rNum blocks processed locally: {COUNTER.value} | Global Progress: {api.get_global_progress()}",
            end="",
        )
        response = api.get_available_block()
        if "message" in response:
            print("\n")
            print(response["message"])
            break
        block_id = response["uuid"]
        block_url = response["url"]
        try:
            if not block_url.strip():
                return block_url

            if debug:
                time.sleep(5)
            else:
                output_name = (
                    block_url.split("/")[3]
                    + "_"
                    + block_url.split("/")[-1].replace(".warc.wat.gz", ".jsonl.wat.gz")
                )
                dir_name = block_url.split("/")[1]

                pathlib.Path(f"{output_path}/{dir_name}/").mkdir(
                    parents=True, exist_ok=True
                )
                subprocess.run(
                    [
                        "./commoncrawl_filter_bin",
                        "http://commoncrawl.s3.amazonaws.com/" + url,
                        f"{output_path}/{dir_name}/{output_name}".strip(),
                    ],
                    timeout=1200,
                    check=True,
                )
            api.mark_block_complete(block_id)
            COUNTER.value += 1
        except Exception as e:
            print(e)
            print(f"Error processing block {block_id}")
            api.mark_block_failed(block_id)
            traceback.print_exc()
            break


def parse_args():
    parser = argparse.ArgumentParser(
        "Download common crawl blocks & parse out CC licensed images"
    )
    parser.add_argument("--processes", type=str, default=None)
    parser.add_argument("--warc_urls_path", type=str, default="./warc_urls.txt")
    parser.add_argument("--out_dir", type=str, default="./output")
    args = parser.parse_args()
    if args.processes is None:
        args.processes = multiprocessing.cpu_count()
    return args


if __name__ == "__main__":
    args = parse_args()
    p = multiprocessing.Pool(args.processes)
    process = partial(process_wats, output_path=args.out_dir)
    total_blocks = api.get_block_count()
    try:
        for _ in range(args.processes):
            p.apply_async(process)
            # stagger the processes
            time.sleep(1)
    finally:
        # wait for all processes to complete
        p.close()
        p.join()
