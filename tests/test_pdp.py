
import os
import json
import time
import pandas as pd
import pytest
from pdp import PDplus
import baseline
import utils
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


def test_read_creates_single_cached_page(tmp_path, monkeypatch):
    csv_path = tmp_path / "small.csv"
    pd.DataFrame(
        [
            {"a": "1", "b": "2"},
            {"a": "3", "b": "4"},
        ]
    ).to_csv(csv_path, index=False)

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(utils, "estimate_row_size", lambda _: 10)
    monkeypatch.setattr(utils, "get_size_limit", lambda: 10_000)

    df = PDplus(str(csv_path))

    assert df.can_fit_in_mem is True
    assert len(df.chunks) == 1
    assert os.path.exists(df.index)
    assert os.path.exists(df.chunks[0])

    with open(df.index, "r") as f:
        pages = json.load(f)

    assert pages == df.chunks


def test_read_splits_into_multiple_pages(tmp_path, monkeypatch):
    csv_path = tmp_path / "large.csv"
    pd.DataFrame(
        [
            {"a": str(i), "b": str(i + 100)}
            for i in range(6)
        ]
    ).to_csv(csv_path, index=False)

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(utils, "estimate_row_size", lambda _: 10)
    monkeypatch.setattr(utils, "get_size_limit", lambda: 20)

    df = PDplus(str(csv_path))

    assert df.can_fit_in_mem is False
    assert df.page_row_capacity == 2
    assert len(df.chunks) == 3
    assert all(os.path.exists(chunk) for chunk in df.chunks)



def test_commit_writes_cached_changes_back_to_csv(tmp_path, monkeypatch):
    csv_path = tmp_path / "commit.csv"
    pd.DataFrame(
        [
            {"a": "1", "b": "2"},
            {"a": "3", "b": "4"},
        ]
    ).to_csv(csv_path, index=False)

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(utils, "estimate_row_size", lambda _: 10)
    monkeypatch.setattr(utils, "get_size_limit", lambda: 10_000)

    df = PDplus(str(csv_path))
    cached = pd.read_pickle(df.chunks[0])
    cached.loc[0, "a"] = "999"
    pd.to_pickle(cached, df.chunks[0])

    df.commit()

    reloaded = pd.read_csv(csv_path, dtype=str)
    assert reloaded.loc[0, "a"] == "999"
    assert list(reloaded.columns) == ["a", "b"]



def test_abort_removes_cached_pages_and_index(tmp_path, monkeypatch):
    csv_path = tmp_path / "abort.csv"
    pd.DataFrame(
        [
            {"a": "1", "b": "2"},
            {"a": "3", "b": "4"},
        ]
    ).to_csv(csv_path, index=False)

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(utils, "estimate_row_size", lambda _: 10)
    monkeypatch.setattr(utils, "get_size_limit", lambda: 10_000)

    df = PDplus(str(csv_path))
    chunk_paths = list(df.chunks)
    page_folder = df.page_folder
    index_path = df.index

    df.abort()

    assert not os.path.exists(index_path)
    assert all(not os.path.exists(chunk) for chunk in chunk_paths)
    assert not os.path.exists(page_folder)


# --- Additional tests for future functionality ---
import pytest

@pytest.mark.xfail(reason="Future functionality: delete not implemented yet")
def test_delete_removes_row_and_updates_page_contents(tmp_path, monkeypatch):
    csv_path = tmp_path / "delete_one.csv"
    pd.DataFrame(
        [
            {"a": "1", "b": "10"},
            {"a": "2", "b": "20"},
            {"a": "3", "b": "30"},
        ]
    ).to_csv(csv_path, index=False)

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(utils, "estimate_row_size", lambda _: 10)
    monkeypatch.setattr(utils, "get_size_limit", lambda: 10_000)

    df = PDplus(str(csv_path))
    df.delete(lambda row: row["a"] == "2")
    df.commit()

    reloaded = pd.read_csv(csv_path, dtype=str)
    assert reloaded.to_dict("records") == [
        {"a": "1", "b": "10"},
        {"a": "3", "b": "30"},
    ]


@pytest.mark.xfail(reason="Future functionality: delete not implemented yet")
def test_delete_across_multiple_pages_removes_only_matching_rows(tmp_path, monkeypatch):
    csv_path = tmp_path / "delete_multi_page.csv"
    pd.DataFrame(
        [
            {"a": str(i), "b": str(i * 10)}
            for i in range(6)
        ]
    ).to_csv(csv_path, index=False)

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(utils, "estimate_row_size", lambda _: 10)
    monkeypatch.setattr(utils, "get_size_limit", lambda: 20)

    df = PDplus(str(csv_path))
    df.delete(lambda row: int(row["a"]) % 2 == 0)
    df.commit()

    reloaded = pd.read_csv(csv_path, dtype=str)
    assert reloaded.to_dict("records") == [
        {"a": "1", "b": "10"},
        {"a": "3", "b": "30"},
        {"a": "5", "b": "50"},
    ]


@pytest.mark.xfail(reason="Future functionality: sum not implemented yet")
def test_sum_returns_total_for_numeric_column(tmp_path, monkeypatch):
    csv_path = tmp_path / "sum.csv"
    pd.DataFrame(
        [
            {"a": "1", "b": "10"},
            {"a": "2", "b": "20"},
            {"a": "3", "b": "30"},
        ]
    ).to_csv(csv_path, index=False)

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(utils, "estimate_row_size", lambda _: 10)
    monkeypatch.setattr(utils, "get_size_limit", lambda: 10_000)

    df = PDplus(str(csv_path))

    assert df.sum("b") == 60


@pytest.mark.xfail(reason="Future functionality: select not implemented yet")
def test_select_returns_only_matching_rows(tmp_path, monkeypatch):
    csv_path = tmp_path / "select.csv"
    pd.DataFrame(
        [
            {"a": "1", "b": "10"},
            {"a": "2", "b": "20"},
            {"a": "3", "b": "30"},
        ]
    ).to_csv(csv_path, index=False)

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(utils, "estimate_row_size", lambda _: 10)
    monkeypatch.setattr(utils, "get_size_limit", lambda: 10_000)

    df = PDplus(str(csv_path))
    selected = df.select(lambda row: int(row["b"]) >= 20)

    if isinstance(selected, pd.DataFrame):
        records = selected.astype(str).to_dict("records")
    else:
        records = list(selected)

    assert records == [
        {"a": "2", "b": "20"},
        {"a": "3", "b": "30"},
    ]
