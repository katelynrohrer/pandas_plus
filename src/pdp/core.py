
import os
from typing import Dict
import pandas as pd

from . import utils
from . import build
from . import page
from . import cache
from . import operations


class PDplus:
    def __init__(self, filepath, sort=True, sort_col=None):
        self.file = filepath
        self.basename = os.path.basename(filepath).rsplit(".")[0]
        self.filesize = os.path.getsize(filepath)

        self.row_size = utils.estimate_row_size(filepath)
        self.page_row_capacity = int(utils.get_size_limit() / self.row_size)

        self.can_fit_in_mem = self.filesize < utils.get_size_limit()

        self.cache_root = os.path.join("tmp", "pages")
        os.makedirs(self.cache_root, exist_ok=True)

        self.page_key = f"{self.basename}_{self.filesize}_{self.page_row_capacity}"
        self.page_folder = os.path.join(self.cache_root, self.page_key)
        self.index = os.path.join(self.page_folder, "index.json")
        os.makedirs(self.page_folder, exist_ok=True)

        self.columns = list(pd.read_csv(self.file, nrows=0).columns)

        if sort and sort_col:
            if sort_col not in self.columns:
                raise ValueError(f"column to be sorted on must exist within df.\ncurrent columns:\n{self.columns}")
            self.sort_by = sort_col
        elif sort and not sort_col:
            self.sort_by = self.columns[0]
        else:
            self.sort_by = None

        self.pages = self.read()

#### PUBLIC API ####

    def read(self):
        pages = cache.load_valid_index(self)
        if pages is not None:
            return pages

        cache.clear_current_cache(self)
        self._clear_old_cache()

        pages = build.build_pages(self)
        self._write_index(pages)
        return pages

    def insert(self, row: Dict):
        return operations.insert(self, row)

    def delete(self, key, single=True, key_col=None):
        return operations.delete(self, key, single, key_col)

    def commit_cache(self):
        return cache.commit_cache(self)

    def abort_cache(self):
        return cache.abort_cache(self)

    def print(self):
        for page in self.pages:
            df = pd.read_pickle(page["path"])
            print(df)

#### HELPER FUNCTIONS ####

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

    def _split_page(self, idx, df):
        print("splitting page")
        return page.split_page(self, idx, df)

    def _insert_sorted_row(self, df, row):
        return operations._insert_sorted_row(self, df, row)

    def _refresh_page_index(self, page_idx, page_df):
        return operations._refresh_page_index(self, page_idx, page_df)

    def _clear_old_cache(self):
        return cache.clear_old_cache(self)

    def _write_index(self, pages):
        return cache.write_index(self, pages)

    def _find_page_index(self, value):
        return page.find_page_index(self, value)

    def _find_page_index_binary(self, value):
        return page.find_page_index_binary(self, value)

    def _rewrite_page(self, idx, df):
        return page.rewrite_page(self, idx, df)

    def _load_page(self, idx):
        return page.load_page(self, idx)

    def _remove_empty_pages(self):
        return page.remove_empty_pages(self)

    def _page_is_full(self, df):
        return page.page_is_full(self, df)
