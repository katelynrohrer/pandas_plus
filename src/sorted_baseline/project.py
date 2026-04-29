

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
    start = time.time()

    file = DATA_FILES[3]

    mem, _ = resource.getrlimit(resource.RLIMIT_AS)  # virtual limits (e.g. the 1gb limit)
    if mem == -1:
        mem = psutil.virtual_memory().available
    size_limit = mem / 15

    sample_df = pd.read_csv(file, dtype=str, nrows=1000)
    columns = sample_df.columns
    row_size = sample_df.memory_usage(deep=True).sum() / len(sample_df)

    # choose subset of columns to project # TODO
    projected_cols = list(columns[:3])

    ## SORTING ##
    # requires several temp files because
    # sorting a CSV larger than memory
    # cannot be done in-place

    ## PROJECT ##
    # ** requires one output file **
    chunk_size = max(1, int(size_limit / row_size))

    folder = os.path.dirname(file)
    name = os.path.basename(file)
    root, ext = os.path.splitext(name)
    output_file = os.path.join(folder, f"{root}_projected{ext}")

    with open(output_file, "w", newline="") as out:
        pd.DataFrame(columns=projected_cols).to_csv(out, index=False)

        for df in pd.read_csv(file, dtype=str, chunksize=chunk_size):
            projected = df[projected_cols]
            projected.to_csv(out, header=False, index=False)

    end = time.time()

    output = f"File: {file}\n" + \
             f"Projected Columns: {projected_cols}\n" + \
             f"Time: {(end - start) * 1000:.2f} ms\n\n"

    print(output)

    with open("./metrics/baseline_1gb_project.txt", "a") as file:
        file.write(output)

if __name__ == "__main__":
    main()