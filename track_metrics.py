
import baseline
import main as m

import os
import sys
import time
import psutil


def track(func):
    process = psutil.Process(os.getpid())

    io_before = process.io_counters()
    start_time = time.time()

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
        track(m.main)
    elif filename == "baseline":
        start, step = baseline.make_stepper()
        start()
        while True:
            try:
                track(step)
            except StopIteration:
                break
    else:
        print("Usage: python3 track_metrics.py [main|baseline]")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python3 track_metrics.py [main|baseline]")
        sys.exit(1)

    file = sys.argv[1]
    main(file)