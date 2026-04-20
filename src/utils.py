
import os
import psutil
import resource
import pandas as pd


def to_mb(bytes_val):
    return round(bytes_val / (1024 * 1024))


DATA_FILES = [
    "data/small_song.csv",
    "data/small_crime.csv",
    "data/medium_la_crime.csv", # unreliably small enough
    "data/medium_song.csv",
    "data/large_song.csv",
    # "data/large_la_crime.csv" # TODO too big for casual testing
]


def iter_files(file_main, files=None):
    if files is None:
        files = DATA_FILES

    for file in files:
        file_main(file)
        yield file


def make_stepper(file_main, files=None):
    gen = None

    def start():
        nonlocal gen
        gen = iter_files(file_main, files)

    def step():
        nonlocal gen
        if gen is None:
            raise StopIteration
        return next(gen)

    return start, step


def run_all(file_main, files=None):
    if files is None:
        files = DATA_FILES

    for file in files:
        print(file)
        file_main(file)
        print()


def get_size_limit():
    mem, _ = resource.getrlimit(resource.RLIMIT_AS)  # virtual limits (e.g. the 1gb limit)

    if mem == -1:
        # in real use case, this would always be used by default
        mem = psutil.virtual_memory().available # physical limits (e.g container capacity)

    size_limit = mem / 15  # pandas typically needs 2-5x the space of the file. we're being conservative here

    return size_limit

def estimate_row_size(file, sample=1000):
    sample_df = pd.read_csv(file, dtype=str, nrows=sample)
    bytes_per_row = sample_df.memory_usage(deep=True).sum() / len(sample_df)
    return bytes_per_row



