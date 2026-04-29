
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
    key_to_delete = "7zzwQwN3jNiK46B2M9kL2Q"
    start = time.time()

    file = DATA_FILES[3]

    mem, _ = resource.getrlimit(resource.RLIMIT_AS)  # virtual limits (e.g. the 1gb limit)
    if mem == -1:
        # in real use case, this would always be used by default
        mem = psutil.virtual_memory().available  # physical limits (e.g container capacity)
    size_limit = mem / 15  # pandas typically needs 2-5x the space of the file. we're being conservative here

    sample_df = pd.read_csv(file, dtype=str, nrows=1000)
    columns = sample_df.columns
    row_size = sample_df.memory_usage(deep=True).sum() / len(sample_df)
    col = columns[0]
    chunk_size = max(1, int(size_limit / row_size))

    folder = os.path.dirname(file)
    name = os.path.basename(file)
    root, ext = os.path.splitext(name)

    ## SORTED DELETE ##
    # ** requires one temp file **
    output_file = os.path.join(folder, f"{root}_deleted{ext}")
    temp_file = os.path.join(folder, f".{root}_deleted{ext}.tmp")

    columns = pd.read_csv(file, dtype=str, nrows=0).columns
    deleted = False

    with open(temp_file, "w", newline="") as out:
        pd.DataFrame(columns=columns).to_csv(out, index=False)

        for df in pd.read_csv(file, dtype=str, chunksize=chunk_size):
            if not deleted:
                matches = df[col] == key_to_delete

                if matches.any():
                    delete_idx = matches.idxmax()
                    keep_df = df.drop(index=delete_idx)
                    keep_df.to_csv(out, header=False, index=False)
                    deleted = True
                else:
                    df.to_csv(out, header=False, index=False)
            else:
                df.to_csv(out, header=False, index=False)

    os.replace(temp_file, output_file)

    if os.path.exists(temp_file):
        os.remove(temp_file)

    end = time.time()

    output = f"File: {file}\n" + \
             f"Deleted: {key_to_delete}\n" + \
             f"Found: {deleted}\n" + \
             f"Time: {(end - start) * 1000:.2f} ms\n\n"

    print(output)

    with open("./metrics/baseline_1gb_delete.txt", "a") as file:
        file.write(output)


if __name__ == "__main__":
    main()