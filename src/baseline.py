
import os
import utils
import pandas as pd


def make_dfs(file):
    size_limit = utils.get_size_limit()

    filesize = os.path.getsize(file)
    if filesize < size_limit:
        df = pd.read_csv(file, dtype=str)
        return [df]
    else:
        row_size = utils.estimate_row_size(file)
        dfs = pd.read_csv(file, dtype=str, chunksize = int(size_limit/row_size) )
        return dfs


def lookup(file, key, key_col):
    # LIMITATION: assumes lookup matches will fit in memory
    size_limit = utils.get_size_limit()
    row_size = utils.estimate_row_size(file)
    chunk_size = max(1, int(size_limit / row_size))

    matches = []
    for df in pd.read_csv(file, dtype=str, chunksize=chunk_size):
        chunk_matches = df[df[key_col] == key]
        if not chunk_matches.empty:
            matches.append(chunk_matches)

    if not matches:
        return pd.DataFrame()

    return pd.concat(matches, ignore_index=True)


def insert(file, row):
    # inserts at the top for speed
    columns = pd.read_csv(file, dtype=str, nrows=0).columns
    row_df = pd.DataFrame([row], columns=columns)
    row_df.to_csv(file, mode="a", header=False, index=False)


def sorted_insert(file, row, sorted_col):
    # assumes the file is already sorted!!
    # **requires a temp file**
    # not sure if this is true baseline in this case
    size_limit = utils.get_size_limit()
    row_size = utils.estimate_row_size(file)
    chunk_size = max(1, int(size_limit / row_size))

    folder = os.path.dirname(file)
    name = os.path.basename(file)
    temp_file = os.path.join(folder, f".{name}.tmp")

    columns = pd.read_csv(file, dtype=str, nrows=0).columns
    row_df = pd.DataFrame([row], columns=columns)
    inserted = False

    try:
        with open(temp_file, "w", newline="") as out:
            pd.DataFrame(columns=columns).to_csv(out, index=False)

            for df in pd.read_csv(file, dtype=str, chunksize=chunk_size):
                if not inserted and row[sorted_col] <= df[sorted_col].iloc[-1]:
                    before = df[df[sorted_col] < row[sorted_col]]
                    after = df[df[sorted_col] >= row[sorted_col]]

                    before.to_csv(out, header=False, index=False)
                    row_df.to_csv(out, header=False, index=False)
                    after.to_csv(out, header=False, index=False)
                    inserted = True
                else:
                    df.to_csv(out, header=False, index=False)

            if not inserted:
                row_df.to_csv(out, header=False, index=False)

        os.replace(temp_file, file)
    finally:
        if os.path.exists(temp_file):
            os.remove(temp_file)


def sort(file, col):
    # requires temp files because sorting a CSV larger than memory cannot be done in-place
    size_limit = utils.get_size_limit()
    row_size = utils.estimate_row_size(file)
    chunk_size = max(1, int(size_limit / row_size))

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

        os.replace(output_file, file)
        return True
    finally:
        if os.path.exists(output_file):
            os.remove(output_file)

        for run_file in run_files:
            if os.path.exists(run_file):
                os.remove(run_file)


def delete(file, key, key_col, single=True):
    # requires a temp file because deleting from a CSV cannot be done in-place
    # **requires a temp file**
    # not sure if this is true baseline in this case
    size_limit = utils.get_size_limit()
    row_size = utils.estimate_row_size(file)
    chunk_size = max(1, int(size_limit / row_size))

    folder = os.path.dirname(file)
    name = os.path.basename(file)
    temp_file = os.path.join(folder, f".{name}.tmp")

    columns = pd.read_csv(file, dtype=str, nrows=0).columns
    deleted = []
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
                    deleted.append(df.loc[[delete_idx]])
                    df = df.drop(index=delete_idx)
                    found = True
                else:
                    deleted.append(matches)
                    df = df[df[key_col] != key]

                df.to_csv(out, header=False, index=False)

        os.replace(temp_file, file)
    finally:
        if os.path.exists(temp_file):
            os.remove(temp_file)

    if not deleted:
        return pd.DataFrame()

    return pd.concat(deleted, ignore_index=True)


def filter(file, predicate):
    # LIMITATION: assumes filter results will fit in memory
    size_limit = utils.get_size_limit()
    row_size = utils.estimate_row_size(file)
    chunk_size = max(1, int(size_limit / row_size))

    matches = []
    for df in pd.read_csv(file, dtype=str, chunksize=chunk_size):
        chunk_matches = df[df.apply(predicate, axis=1)]
        if not chunk_matches.empty:
            matches.append(chunk_matches)

    if not matches:
        return pd.DataFrame()

    return pd.concat(matches, ignore_index=True)


def project(file, cols):
    # LIMITATION: assumes projected result will fit in memory
    size_limit = utils.get_size_limit()
    row_size = utils.estimate_row_size(file)
    chunk_size = max(1, int(size_limit / row_size))

    projected = []
    for df in pd.read_csv(file, dtype=str, chunksize=chunk_size):
        projected.append(df[cols])

    if not projected:
        return pd.DataFrame(columns=cols)

    return pd.concat(projected, ignore_index=True)


def count(file, predicate):
    size_limit = utils.get_size_limit()
    row_size = utils.estimate_row_size(file)
    chunk_size = max(1, int(size_limit / row_size))

    total = 0
    for df in pd.read_csv(file, dtype=str, chunksize=chunk_size):
        total += df.apply(predicate, axis=1).sum()

    return total


def file_main(file):
    dfs = make_dfs(file)
    for df in dfs:
        # pass
        print(df)
    del dfs


def make_stepper():
    return utils.make_stepper(file_main)


def main():
    utils.run_all(file_main)


if __name__ == "__main__":
    main()