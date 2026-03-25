
import os
import psutil
import resource

import pandas as pd


def to_mb(bytes_val):
    return round(bytes_val / (1024 * 1024))


def get_size_limit():
    mem, _ = resource.getrlimit(resource.RLIMIT_AS)  # virtual limits (e.g. the 1gb limit)

    if mem == -1:
        # in real use case, this would always be used by default
        mem = psutil.virtual_memory().available # physical limits (e.g container capacity)

    size_limit = mem / 5  # pandas typically needs 2-5x the space of the file. we're being conservative here

    return size_limit


def estimate_row_size(file, sample=1000):
    sample_df = pd.read_csv(file, dtype=str, nrows=sample)
    bytes_per_row = sample_df.memory_usage(deep=True).sum() / len(sample_df)
    return bytes_per_row * 3 # give an extra 3x buffer


def make_dfs(file):
    size_limit = get_size_limit()

    filesize = os.path.getsize(file)
    if filesize < size_limit:
        df = pd.read_csv(file, dtype=str)
        return [df]
    else:
        row_size = estimate_row_size(file)
        dfs = pd.read_csv(file, dtype=str, chunksize = int(size_limit/row_size) )
        return dfs


def make_stepper():
    gen = None

    def start():
        nonlocal gen
        gen = step_files()

    def step():
        nonlocal gen
        if gen is None:
            raise StopIteration
        return next(gen)

    return start, step


def step_files():
    files = [
        "data/small_song.csv",
        "data/small_la_crime.csv",
        "data/medium_song.csv",
        "data/large_la_crime.csv",
        "data/large_song.csv",
    ]

    for file in files:
        print(file)
        dfs = make_dfs(file)
        for df in dfs:
            pass
        del dfs

        yield file


def main():
    for file in ["data/small_song.csv", "data/small_la_crime.csv", "data/medium_song.csv", "data/large_la_crime.csv", "data/large_song.csv"]:
        print(file)
        dfs = make_dfs(file)
        for df in dfs:
            pass
        del dfs

        print()


if __name__ == "__main__":
    main()