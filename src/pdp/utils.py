
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

    size_limit = mem / 15  # pandas typically needs 2-5x the space of the file. we're being conservative here

    return size_limit

def estimate_row_size(file, sample=1000):
    sample_df = pd.read_csv(file, dtype=str, nrows=sample)
    bytes_per_row = sample_df.memory_usage(deep=True).sum() / len(sample_df)
    return bytes_per_row



