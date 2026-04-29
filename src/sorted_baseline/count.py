import os
import time
import psutil
import resource
import pandas as pd


DATA_FILES = [
    "data/small_song_sorted.csv",
    "data/small_crime_sorted.csv",
    "data/medium_crime_sorted.csv",
    "data/medium_song_sorted.csv",

    # these do not exist yet
    # "data/large_crime_sorted.csv",
    # "data/large_song_sorted.csv",
]


def main():
    predicate_str = 'lambda x: x["id"].startswith("7z") and float(x["avg_artist_popularity"]) > 50'
    start = time.time()

    file = DATA_FILES[3]

    mem, _ = resource.getrlimit(resource.RLIMIT_AS)
    if mem == -1:
        mem = psutil.virtual_memory().available
    size_limit = mem / 15

    sample_df = pd.read_csv(file, dtype=str, nrows=1000)
    columns = sample_df.columns
    row_size = sample_df.memory_usage(deep=True).sum() / len(sample_df)
    col = columns[0]

    predicate = eval(predicate_str)

    ## COUNT ##
    chunk_size = max(1, int(size_limit / row_size))

    count = 0

    for df in pd.read_csv(file, dtype=str, chunksize=chunk_size):
        matches = df.apply(predicate, axis=1)
        count += matches.sum()

    end = time.time()

    output = f"File: {file}\n" + \
             f"Predicate: {predicate_str}\n" + \
             f"Count: {count}\n" + \
             f"Time: {(end - start) * 1000:.2f} ms\n\n"

    print(output)

    with open("./metrics/baseline_1gb_count.txt", "a") as file:
        file.write(output)


if __name__ == "__main__":
    main()