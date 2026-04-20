
import time
import pytest
import baseline
from utils import DATA_FILES

def test_small1_time():
    start = time.time()
    baseline.file_main("data/small_song.csv")
    end = time.time()
    assert end - start < 1, f"time elapsed: {end-start}"


def test_small2_time():
    start = time.time()
    baseline.file_main("data/small_crime.csv")
    end = time.time()
    assert end - start < 1, f"time elapsed: {end-start}"

def test_medium1_time():
    start = time.time()
    baseline.file_main("data/medium_la_crime.csv")
    end = time.time()
    assert end - start < 3, f"time elapsed: {end-start}"


def test_medium2_time():
    start = time.time()
    baseline.file_main("data/medium_song.csv")
    end = time.time()
    assert end - start < 7, f"time elapsed: {end-start}"

#
# def test_large1_time():
#     start = time.time()
#     baseline.file_main("data/large_song.csv")
#     end = time.time()
#     assert end - start < 90, f"time elapsed: {end - start}"
#
#
# def test_large2_time():
#     start = time.time()
#     baseline.file_main("data/large_la_crime.csv")
#     end = time.time()
#     assert end - start < 140, f"time elapsed: {end - start}"

def count(dfs):
    iters = 0
    for _ in dfs:
        iters += 1
    return iters

def test_small1_memory():
    dfs = baseline.make_dfs("data/small_song.csv")
    assert count(dfs) == 1, f"actually {count(dfs)}"

def test_small2_memory():
    dfs = baseline.make_dfs("data/small_song.csv")
    assert count(dfs) == 1, f"actually {count(dfs)}"

def test_medium1_memory():
    dfs = baseline.make_dfs("data/medium_la_crime.csv")
    assert count(dfs) == 24, f"actually {count(dfs)}"

def test_medium2_memory():
    dfs = baseline.make_dfs("data/medium_song.csv")
    assert count(dfs) == 23, f"actually {count(dfs)}"
