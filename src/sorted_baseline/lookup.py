
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
    key_to_lookup = "7zzwQwN3jNiK46B2M9kL2Q"
    start = time.time()

    file = DATA_FILES[3]

    mem, _ = resource.getrlimit(resource.RLIMIT_AS)  # virtual limits (e.g. the 1gb limit)
    if mem == -1:
        # in real use case, this would always be used by default
        mem = psutil.virtual_memory().available # physical limits (e.g container capacity)
    size_limit = mem / 15  # pandas typically needs 2-5x the space of the file. we're being conservative here

    sample_df = pd.read_csv(file, dtype=str, nrows=1000)
    columns = sample_df.columns
    row_size = sample_df.memory_usage(deep=True).sum() / len(sample_df)
    col = columns[0]

    ## SORTING ##
    # requires several temp files because
    # sorting a CSV larger than memory
    # cannot be done in-place

    ## SORTED LOOKUP ##
    chunk_size = max(1, int(size_limit / row_size))

    found = False
    found_row = None

    for df in pd.read_csv(file, dtype=str, chunksize=chunk_size):
        if key_to_lookup <= df[col].iloc[-1]:
            matches = df[df[col] == key_to_lookup]

            if not matches.empty:
                found = True
                found_row = matches.iloc[0]

            break

    end = time.time()

    output = f"File: {file}\n" + \
             f"Looked up: {key_to_lookup}\n" + \
             f"Found: {found}\n" + \
             f"Time: {(end - start) * 1000:.2f} ms\n\n"

    print(output)

    with open("./metrics/baseline_1gb_lookup.txt", "a") as file:
        file.write(output)

if __name__ == "__main__":
    main()