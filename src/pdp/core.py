import os
from typing import Dict
import pandas as pd

from . import utils
from . import build
from . import page
from . import cache


class PDplus:
    # TODO add a sort option and a sort by col
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

        self.columns = pd.read_csv(self.file, nrows=0).columns
        self.chunks = self.read()


    def clear_old_cache(self):
        return cache.clear_old_cache(self)


    def read(self):
        pages = cache.load_valid_index(self)
        if pages is not None:
            return pages

        cache.clear_current_cache(self)
        self.clear_old_cache()

        pages = build.build_pages(self)
        self._write_index(pages)
        return pages

    def _remove_empty_pages(self):
        return page.remove_empty_pages(self)

    def _bucket_key(self, value):
        return build.bucket_key(value)

    def _pages_from_buckets(self):
        return build.pages_from_buckets(self)

    def _pages_from_df(self, df):
        return build.pages_from_df(self, df)

    def _sort_df(self, df):
        return build.sort_df(self, df)

    def _page_bounds(self, df):
        return page.page_bounds(self, df)

    def _page_filename(self, df):
        return page.page_filename(self, df)

    def _write_page(self, df, idx=None):
        return page.write_page(self, df, idx)

    def _write_index(self, pages):
        return cache.write_index(self, pages)

    def _find_page_index(self, value):
        return page.find_page_index(self, value)

    def _rewrite_page(self, idx, df):
        return page.rewrite_page(self, idx, df)

    def _split_page(self, idx):
        return page.split_page(self, idx)

    def _load(self, idx):
        return page.load_page(self, idx)

    def insert(self, row: Dict):
        if set(row.keys()) != set(self.columns):
            raise KeyError("New row must have the same columns as the rest of the df")

        row_value = row[self.columns[0]]

        if not self.chunks:
            new_page = self._write_page(pd.DataFrame([row], columns=self.columns))
            self.chunks.append(new_page)
            self._write_index(self.chunks)
            return

        page_idx = self._find_page_index(row_value)
        page_df = self._load(page_idx)
        page_df.loc[len(page_df)] = row
        page_df = self._sort_df(page_df)
        self._rewrite_page(page_idx, page_df)

        if self.page_is_full(page_df):
            self._split_page(page_idx)
        else:
            self._write_index(self.chunks)


    def page_is_full(self, df):
        return page.page_is_full(self, df)


    def commit(self):
        first_page = True
        for page in self.chunks:
            df = pd.read_pickle(page["path"])
            df.to_csv(
                self.file,
                mode="w" if first_page else "a",
                index=False,
                header=first_page,
            )
            first_page = False



    def abort(self):
        return cache.abort_cache(self)


    def close(self):
        self.abort()


    def print(self):
        for page in self.chunks:
            df = pd.read_pickle(page["path"])
            print(df)
