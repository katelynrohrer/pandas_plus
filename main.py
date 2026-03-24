import pandas as pd
import psutil
import time
import os
from icecream import ic

process = psutil.Process(os.getpid())
io_before = process.io_counters()

start_time = time.time()

df = pd.read_csv("data/medium_song_songs.csv")

end_time = time.time()
io_after = process.io_counters()

info = process.memory_full_info()

print(f"Time: {end_time - start_time:.2f}s")
print(f"RSS: {info.rss / 1024**2:.2f} MB")
print(f"Read bytes: {(io_after.read_bytes - io_before.read_bytes) / 1024**2:.2f} MB")
print(f"Write bytes: {(io_after.write_bytes - io_before.write_bytes) / 1024**2:.2f} MB")