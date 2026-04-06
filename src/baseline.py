
import os

import pandas as pd
import utils


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


def file_main(file):
    dfs = make_dfs(file)
    for df in dfs:
        print(df)
    del dfs


def make_stepper():
    return utils.make_stepper(file_main)


def main():
    utils.run_all(file_main)


if __name__ == "__main__":
    main()