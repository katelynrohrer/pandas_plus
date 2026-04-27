
import os
import time
import resource
import pandas as pd


DATA_FILES = [
    "data/small_song.csv",
    "data/small_crime.csv",
    "data/medium_crime.csv",
    "data/medium_song.csv",
    "data/large_crime.csv",
    # "data/large_song.csv", # takes too long to test
]





def main():

    start = time.time()

    file = DATA_FILES[0]

    mem, _ = resource.getrlimit(resource.RLIMIT_AS)  # virtual limits (e.g. the 1gb limit)
    if mem == -1:
        # in real use case, this would always be used by default
        mem = psutil.virtual_memory().available # physical limits (e.g container capacity)
    size_limit = mem / 15  # pandas typically needs 2-5x the space of the file. we're being conservative here

    sample_df = pd.read_csv(file, dtype=str, nrows=1000)
    columns = sample_df.columns
    row_size = sample_df.memory_usage(deep=True).sum() / len(sample_df)
    chunk_size = max(1, int(size_limit / row_size))

    col = columns[0]


    ## SORTING ##
    # requires several temp files because
    # sorting a CSV larger than memory
    # cannot be done in-place
    folder = os.path.dirname(file)
    name = os.path.basename(file)
    output_file = os.path.join(folder, f".{name}.sorted.tmp")
    run_files = []

    try:
        columns = pd.read_csv(file, dtype=str, nrows=0).columns

        for i, df in enumerate(pd.read_csv(file, dtype=str, chunksize=chunk_size)):
            run_file = os.path.join(folder, f".{name}.run_{i}.tmp")
            df = df.sort_values(by=col, kind="stable").reset_index(drop=True)
            df.to_csv(run_file, index=False)
            run_files.append(run_file)

        readers = [pd.read_csv(run_file, dtype=str, chunksize=1) for run_file in run_files]
        current_rows = []

        for reader in readers:
            try:
                current_rows.append(next(reader))
            except StopIteration:
                current_rows.append(None)

        with open(output_file, "w", newline="") as out:
            pd.DataFrame(columns=columns).to_csv(out, index=False)

            while any(row is not None for row in current_rows):
                min_idx = None
                min_val = None

                for i, row_df in enumerate(current_rows):
                    if row_df is None:
                        continue

                    val = row_df.iloc[0][col]
                    if min_val is None or val < min_val:
                        min_val = val
                        min_idx = i

                current_rows[min_idx].to_csv(out, header=False, index=False)

                try:
                    current_rows[min_idx] = next(readers[min_idx])
                except StopIteration:
                    current_rows[min_idx] = None

        base, ext = os.path.splitext(file)
        sorted_file = f"{base}_sorted{ext}"
        os.replace(output_file, sorted_file)
    finally:
        if os.path.exists(output_file):
            os.remove(output_file)

        for run_file in run_files:
            if os.path.exists(run_file):
                os.remove(run_file)

    end = time.time()

    output = f"File: {file}\n" + \
             f"Sort: True\n" + \
             f"Time: {end - start:.2f} seconds\n\n"

    print(output)

    with open("./metrics/sorted_baseline.txt", "a") as file:
        file.write(output)

if __name__ == "__main__":
    main()