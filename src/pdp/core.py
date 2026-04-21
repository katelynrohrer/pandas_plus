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
        self.sort_by = build.set_sort_mode(self, sort, sort_col)

        self.pages = None

#### PUBLIC API ####

    def cache_exists(self):
        return os.path.exists(self.index)

    def cache_is_valid(self):
        return cache.cache_is_valid(self)

    def build_cache(self):
        return cache.build_cache(self)

    def commit_cache(self):
        return cache.commit_cache(self)

    def abort_cache(self):
        return cache.abort_cache(self)

    def read_cache(self):
        return cache.read_cache(self)

    def build_pages(self):
        return build.build_pages(self)

    def insert(self, row: Dict):
        return operations.insert(self, row)

    def delete(self, key, single=True, key_col=None):
        return operations.delete(self, key, single, key_col)

    def lookup(self, key, key_col=None):
        return operations.lookup(self, key, key_col)

    def print(self):
        for item in self.pages:
            df = pd.read_pickle(item["path"])
            print(df)


#### HELPER FUNCTIONS ####

    def _sort_df(self, df):
        return build.sort_df(self, df)

    def _write_index(self, pages):
        return cache.write_index(self, pages)

    def _find_page_index_binary(self, value):
        return page.find_page_index_binary(self, value)

    def _scan_for_key(self, key, key_col):
        return page.scan_for_key(self, key, key_col)

    def _write_page(self, df, idx=None):
        return page.write_page(self, df, idx)

    def _split_page(self, idx, df):
        return page.split_page(self, idx, df)

    def _update_page(self, idx, df):
        return page.update_page(self, idx, df)

    def _load_page(self, idx):
        return page.load_page(self, idx)

    def _remove_empty_pages(self):
        return page.remove_empty_pages(self)

    def _page_is_full(self, df):
        return page.page_is_full(self, df)
