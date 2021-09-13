from tqdm import tqdm
from api import API

api = API(host="http://176.9.113.70")
total = api.get_block_count()
pbar = tqdm(total=total)
last_count = 0
while True:
    count = api.get_global_progress()
    pbar.update(count - last_count)
    last_count = count

