
import os
import psutil
import resource

import pandas as pd
import utils


def make_dfs(file):
    size_limit = utils.get_size_limit()

    filesize = os.path.getsize(file)
    if filesize < size_limit:
        df = pd.read_csv(file, dtype=str)
        return [df]
    else:
        row_size = utils.estimate_row_size(file)
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
        file_main(file)
        yield file


def file_main(file):
    dfs = make_dfs(file)
    for df in dfs:
        print(df)
    del dfs


def main():
    for file in ["data/small_song.csv", "data/small_la_crime.csv", "data/medium_song.csv", "data/large_la_crime.csv", "data/large_song.csv"]:
        print(file)
        file_main(file)
        print()


if __name__ == "__main__":
    main()