
import os
import psutil
import resource

import pandas as pd
import utils



class PDplus:
    def __init__(self, filepath):
        self.file = filepath
        self.filesize = os.path.getsize(filepath)

        self.row_size = utils.estimate_row_size(filepath)
        self.chunksize = int(utils.get_size_limit()/self.row_size)

        self.can_fit_in_mem = self.filesize < utils.get_size_limit()
        self.chunk_folder = f"tmp{os.sep}chunks{os.sep}{os.path.basename(self.file)}"
        os.makedirs(os.path.dirname(self.chunk_folder), exist_ok=True)

        self.chunks = self.read()


    def read(self):
        if self.can_fit_in_mem:
            df = pd.read_csv(self.file, dtype=str)
            filename = f"{self.chunk_folder}_0.pickle"
            pd.to_pickle(df, filename)
            return [filename]

        chunks = []
        dfs = pd.read_csv(self.file, dtype=str, chunksize=self.chunksize)
        for i, df in enumerate(dfs):
            filename = f"{self.chunk_folder}_{i}.pickle"
            pd.to_pickle(df, filename)
            chunks.append(filename)
        return chunks


    def print(self):
        for chunk in self.chunks:
            df = pd.read_pickle(chunk)
            print(df)


def make_stepper():
    gen = None

    def start():
        nonlocal gen
        gen = step_files()

    def step():
        nonlocal gen
        if gen is None:
            raise StopIteration
        return next(gen)

    return start, step


def step_files():
    files = [
        "data/small_song.csv",
        "data/small_la_crime.csv",
        "data/medium_song.csv",
        "data/large_la_crime.csv",
        "data/large_song.csv",
    ]

    for file in files:
        file_main(file)
        yield file


def file_main(file):
    df = PDplus(file)
    df.print()


def main():
    for file in ["data/small_song.csv", "data/small_la_crime.csv", "data/medium_song.csv", "data/large_la_crime.csv", "data/large_song.csv"]:
        print(file)
        file_main(file)
        print()


if __name__ == "__main__":
    main()