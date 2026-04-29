import os
import time
import psutil
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


def lookup(file, key, key_col):
    size_limit = get_size_limit()
    row_size = estimate_row_size(file)
    chunk_size = max(1, int(size_limit / row_size))

    folder = os.path.dirname(file)
    name = os.path.basename(file)
    output_file = os.path.join(folder, f"{name}_lookup.csv")

    columns = pd.read_csv(file, dtype=str, nrows=0).columns
    wrote_header = False
    found = False

    if os.path.exists(output_file):
        os.remove(output_file)

    for df in pd.read_csv(file, dtype=str, chunksize=chunk_size):
        chunk_matches = df[df[key_col] == key]
        if chunk_matches.empty:
            continue

        chunk_matches.to_csv(output_file, mode="a", header=not wrote_header, index=False)
        wrote_header = True
        found = True

    if not found:
        pd.DataFrame(columns=columns).to_csv(output_file, index=False)

    return output_file


def insert(file, row):
    # inserts at the bottom for speed
    columns = pd.read_csv(file, dtype=str, nrows=0).columns
    row_df = pd.DataFrame([row], columns=columns)
    row_df.to_csv(file, mode="a", header=False, index=False)


def delete(file, key, key_col, single=True):
    # requires a temp file because deleting from a CSV cannot be done in-place
    size_limit = get_size_limit()
    row_size = estimate_row_size(file)
    chunk_size = max(1, int(size_limit / row_size))

    folder = os.path.dirname(file)
    name = os.path.basename(file)
    temp_file = os.path.join(folder, f".{name}.tmp")

    columns = pd.read_csv(file, dtype=str, nrows=0).columns
    found = False

    try:
        with open(temp_file, "w", newline="") as out:
            pd.DataFrame(columns=columns).to_csv(out, index=False)

            for df in pd.read_csv(file, dtype=str, chunksize=chunk_size):
                matches = df[df[key_col] == key]

                if matches.empty or (single and found):
                    df.to_csv(out, header=False, index=False)
                    continue

                if single:
                    delete_idx = matches.index[0]
                    df = df.drop(index=delete_idx)
                    found = True
                else:
                    df = df[df[key_col] != key]
                    if not matches.empty:
                        found = True

                df.to_csv(out, header=False, index=False)

        os.replace(temp_file, file)
    finally:
        if os.path.exists(temp_file):
            os.remove(temp_file)

    return found


def filter(file, predicate):
    size_limit = get_size_limit()
    row_size = estimate_row_size(file)
    chunk_size = max(1, int(size_limit / row_size))

    folder = os.path.dirname(file)
    name = os.path.basename(file)
    output_file = os.path.join(folder, f"{name}_filter.csv")

    columns = pd.read_csv(file, dtype=str, nrows=0).columns
    wrote_header = False
    found = False

    if os.path.exists(output_file):
        os.remove(output_file)

    for df in pd.read_csv(file, dtype=str, chunksize=chunk_size):
        chunk_matches = df[df.apply(predicate, axis=1)]
        if chunk_matches.empty:
            continue

        chunk_matches.to_csv(output_file, mode="a", header=not wrote_header, index=False)
        wrote_header = True
        found = True

    if not found:
        pd.DataFrame(columns=columns).to_csv(output_file, index=False)

    return output_file


def project(file, cols):
    size_limit = get_size_limit()
    row_size = estimate_row_size(file)
    chunk_size = max(1, int(size_limit / row_size))

    folder = os.path.dirname(file)
    name = os.path.basename(file)
    output_file = os.path.join(folder, f"{name}_project.csv")

    wrote_header = False

    if os.path.exists(output_file):
        os.remove(output_file)

    for df in pd.read_csv(file, dtype=str, usecols=cols, chunksize=chunk_size):
        df.to_csv(output_file, mode="a", header=not wrote_header, index=False)
        wrote_header = True

    if not wrote_header:
        pd.DataFrame(columns=cols).to_csv(output_file, index=False)

    return output_file


def count(file, predicate):
    size_limit = get_size_limit()
    row_size = estimate_row_size(file)
    chunk_size = max(1, int(size_limit / row_size))

    total = 0
    for df in pd.read_csv(file, dtype=str, chunksize=chunk_size):
        total += df.apply(predicate, axis=1).sum()

    return total


def main():
    # 2 (crime) keys:
    # 200200759, 220716694, 252104017
    # 3 (song) keys:
    # 0001piYJu94Ec4hJFytG5G, 3reioH6nZ4rQCbscqVwJt4, 7zzwQwN3jNiK46B2M9kL2Q
    file = DATA_FILES[3]
    key = "3reioH6nZ4rQCbscqVwJt4"
    func = "_delete"

    columns = pd.read_csv(file, dtype=str, nrows=0).columns
    # row = {col: key for col in columns}

    # print(f"File: {file}")
    # print(f"Inserted Key: {key}")

    start = time.time()
    print(delete(file, key, columns[0], single=True))
    end = time.time()
    # print("Insert complete")

    # deleted = delete(file, key, columns[0], single=True)
    # print(f"Deleted Rows: {len(deleted)}")

    output = f"File: {file}\n" + \
             f"Key: {key}\n" + \
             f"Time: {(end - start)*1000:.2f} ms\n\n"

    print(output)

    with open(f"metrics/baseline_unsorted_1gb{func}.txt", "a") as f:
        f.write(output)


if __name__ == "__main__":
    main()