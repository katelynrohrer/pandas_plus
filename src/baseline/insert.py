
import os
import time
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
    char_inserted = "99999999999999999999999999999"
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

    ## SORTED INSERT ##
    # ** requires one temp file **
    row = {k: char_inserted for k in columns}
    chunk_size = max(1, int(size_limit / row_size))

    folder = os.path.dirname(file)
    name = os.path.basename(file)
    root, ext = os.path.splitext(name)
    output_file = os.path.join(folder, f"{root}_inserted{ext}")
    temp_file = os.path.join(folder, f".{root}_inserted{ext}.tmp")

    columns = pd.read_csv(file, dtype=str, nrows=0).columns
    row_df = pd.DataFrame([row], columns=columns)
    inserted = False

    with open(temp_file, "w", newline="") as out:
        pd.DataFrame(columns=columns).to_csv(out, index=False)

        for df in pd.read_csv(file, dtype=str, chunksize=chunk_size):
            if not inserted and row[col] <= df[col].iloc[-1]:
                before = df[df[col] < row[col]]
                after = df[df[col] >= row[col]]

                before.to_csv(out, header=False, index=False)
                row_df.to_csv(out, header=False, index=False)
                after.to_csv(out, header=False, index=False)
                inserted = True
            else:
                df.to_csv(out, header=False, index=False)

        if not inserted:
            row_df.to_csv(out, header=False, index=False)

    os.replace(temp_file, output_file)

    if os.path.exists(temp_file):
        os.remove(temp_file)

    end = time.time()

    output = f"File: {file}\n" + \
             f"Inserted: {char_inserted}\n" + \
             f"Time: {(end - start) * 1000:.2f} ms\n\n"

    print(output)

    with open("./metrics/baseline_1gb_insert.txt", "a") as file:
        file.write(output)

if __name__ == "__main__":
    main()
