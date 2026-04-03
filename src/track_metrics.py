
import baseline
import main as m

import io
import os
import sys
import time
import psutil
from contextlib import redirect_stdout



def track(func):
    process = psutil.Process(os.getpid())

    io_before = process.io_counters()
    start_time = time.time()

    stdout_buffer = io.StringIO()
    with redirect_stdout(stdout_buffer):
        func()

    io_after = process.io_counters()
    end_time = time.time()

    info = process.memory_full_info()

    print(f"Time: {end_time - start_time:.2f}s")
    print(f"RSS: {info.rss / 1024 ** 2:.2f} MB")
    print(f"Read bytes: {(io_after.read_bytes - io_before.read_bytes) / 1024 ** 2:.2f} MB")
    print(f"Write bytes: {(io_after.write_bytes - io_before.write_bytes) / 1024 ** 2:.2f} MB")
    print()

def main(filename):
    if filename == "main":
        start, step = m.make_stepper()
    elif filename == "baseline":
        start, step = baseline.make_stepper()
    else:
        print("Usage: python3 track_metrics.py [main|baseline]")
        return

    start()
    while True:
        try:
            track(step)
        except StopIteration:
            break

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python3 track_metrics.py [main|baseline]")
        sys.exit(1)

    file = sys.argv[1]
    main(file)
