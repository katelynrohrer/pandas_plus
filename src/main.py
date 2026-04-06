
import os
import json
from typing import Dict
import pandas as pd
import utils


class PDplus:
    def __init__(self, filepath):
        self.file = filepath
        self.basename = os.path.basename(filepath).rsplit(".")[0]
        self.filesize = os.path.getsize(filepath)

        self.row_size = utils.estimate_row_size(filepath)
        self.page_row_capacity = int(utils.get_size_limit() / self.row_size)

        self.can_fit_in_mem = self.filesize < utils.get_size_limit()

        self.cache_root = os.path.join("tmp", "chunks")
        os.makedirs(self.cache_root, exist_ok=True)

        self.page_key = f"{self.basename}_{self.filesize}_{self.page_row_capacity}"
        self.page_folder = os.path.join(self.cache_root, self.page_key)
        self.index = os.path.join(self.page_folder, "index.json")
        os.makedirs(self.page_folder, exist_ok=True)

        self.chunks = self.read()
        self.columns = None


    def clear_old_cache(self):
        for name in os.listdir(self.cache_root):
            path = os.path.join(self.cache_root, name)

            if name == self.page_key:
                continue

            if not name.startswith(f"{self.basename}_"):
                continue

            if os.path.isdir(path):
                index = os.path.join(path, "index.json")

                if os.path.exists(index):
                    with open(index, "r") as f:
                        pages = json.load(f)

                    for page in pages:
                        if os.path.exists(page):
                            os.remove(page)

                    os.remove(index)

                if not os.listdir(path):
                    os.rmdir(path)


    def read(self):
        # if a complete page cache already exists, use it as the working copy
        if os.path.exists(self.index):
            with open(self.index, "r") as f:
                pages = json.load(f)

            if all(os.path.exists(page) for page in pages):
                return pages

        # only clear older caches when this cache is missing or invalid
        self.clear_old_cache()

        pages = []
        if self.can_fit_in_mem:
            filename = os.path.join(self.page_folder, "0.pickle")
            df = pd.read_csv(self.file, dtype=str)
            pd.to_pickle(df, filename)
            pages.append(filename)
        else:
            dfs = pd.read_csv(self.file, dtype=str, chunksize=self.page_row_capacity)
            for i, df in enumerate(dfs):
                filename = os.path.join(self.page_folder, f"{i}.pickle")
                pd.to_pickle(df, filename)
                pages.append(filename)

        with open(self.index, "w") as f:
            json.dump(pages, f)

        return pages


    def columns(self):
        if self.columns is None:
            df = self._load(0)
            self.columns = df.columns
        return self.columns


    def _load(self, idx):
        return pd.read_pickle(self.chunks[idx])

    def insert(self, row: Dict):
        if set(row.keys()) != set(self.columns()):
            raise KeyError("New row must have the same columns as the rest of the df")

        # inserting row into page
        last_page = self._load(-1)
        if not self.page_is_full(last_page):
            last_page.loc[len(last_page)] = row
            pd.to_pickle(last_page, self.chunks[-1])
            return

        # making a new page for the row
        df = pd.DataFrame([row], columns=self.columns)
        new_page_num = len(self.chunks)
        filename = os.path.join(self.page_folder, f"{new_page_num}.pickle")
        pd.to_pickle(df, filename)
        self.chunks.append(filename)

        with open(self.index, "w") as f:
            json.dump(self.chunks, f)


    def page_is_full(self, df):
        return len(df) >= self.page_row_capacity


    def commit(self):
        first_page = True
        for page in self.chunks:
            df = pd.read_pickle(page)
            df.to_csv(
                self.file,
                mode="w" if first_page else "a",
                index=False,
                header=first_page,
            )
            first_page = False



    def abort(self):
        # delete all pages in the cache, storage saver
        # use when done working on files in a session
        # -- not recommended to call on each run --
        if os.path.exists(self.index):
            with open(self.index, "r") as f:
                pages = json.load(f)

            for page in pages:
                if os.path.exists(page):
                    os.remove(page)

            os.remove(self.index)

        if os.path.exists(self.page_folder) and not os.listdir(self.page_folder):
            os.rmdir(self.page_folder)


    def close(self):
        self.abort()


    def print(self):
        for page in self.chunks:
            df = pd.read_pickle(page)
            print(df)


def file_main(file):
    df = PDplus(file)
    df.print()


def make_stepper():
    return utils.make_stepper(file_main)


def main():
    utils.run_all(file_main)


if __name__ == "__main__":
    main()