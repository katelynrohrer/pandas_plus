
import time
import pytest
from main import PDplus
import baseline
from utils import DATA_FILES

def test_memory_fit():
    f1 = "data/small_song.csv"
    f2 = "data/small_crime.csv"

    df1 = PDplus(f1)
    assert df1.can_fit_in_mem, f"data/small_song.csv can fit in memory: {df1.can_fit_in_mem}"

    df2 = PDplus(f2)
    assert df2.can_fit_in_mem, f"data/small_crime.csv can fit in memory: {df2.can_fit_in_mem}"


def test_memory_matches_baseline():
    def count(dfs):
        iters = 0
        for _ in dfs:
            iters += 1
        return iters

    for file in DATA_FILES[:2]:  # last two are too slow # TODO test full
        pdp_df = PDplus(file)
        baseline_dfs = baseline.make_dfs(file)
        baseline_chunks = count(baseline_dfs)
        assert pdp_df.can_fit_in_mem == (baseline_chunks == 1)
