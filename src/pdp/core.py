import os
from typing import Dict
import pandas as pd

from . import utils
from . import build
from . import page
from . import cache
from . import operations


class PDplus:
    def __init__(self, filepath, sort=True, sort_col=None, build_name="default"):
        self.file = filepath
        self.basename = os.path.basename(filepath).rsplit(".")[0]
        self.filesize = os.path.getsize(filepath)

        self.row_size = utils.estimate_row_size(filepath)
        self.page_row_capacity = int(utils.get_size_limit() / self.row_size)

        self.can_fit_in_mem = self.filesize < utils.get_size_limit()
        self.build_name = build_name

        self.cache_root = os.path.join("tmp", "builds")
        os.makedirs(self.cache_root, exist_ok=True)

        self.build_key = f"{self.basename}_{self.filesize}_{self.page_row_capacity}"
        self.build_root = os.path.join(self.cache_root, self.build_key)
        self.page_folder = os.path.join(self.build_root, self.build_name)
        self.index = os.path.join(self.page_folder, "index.json")
        self.meta = os.path.join(self.page_folder, "meta.json")
        os.makedirs(self.page_folder, exist_ok=True)

        self.columns = list(pd.read_csv(self.file, nrows=0).columns)
        build.set_sort_mode(self, sort, sort_col)

        self.pages = None
        self._temp_build_counter = 0

#### PUBLIC API ####

    def cache_exists(self):
        # Returns true/false on whether a cache folder exists for this build.
        # Does not validate that the cache is correct.
        # In most cases, cache_is_valid is preferred to use as it does both.
        if os.path.exists(self.index):
            return True
        print(f"WARNING: no existing cache by name {self.build_name}. Reloading from disk")
        return False

    def cache_is_valid(self):
        # Returns true/false on whether a cache folder exists for this build
        # AND that the cache is valid
        # A valid cache means that all files correlated to the build exist
        # and have not been changed since last save
        return cache.cache_is_valid(self)

    def build_cache(self, overwrite=False):
        # Builds the cache from the source csv file. By default,
        # it will raise an error if a cache already exists.
        # If overwrite is true, it will overwrite the existing cache if it exists
        cache.build_cache(self, overwrite)

    def commit_cache(self):
        # Commits the current build back to the source csv file.
        cache.commit_cache(self)

    def abort_cache(self):
        # Deletes the current build entirely
        cache.abort_cache(self)

    def read_cache(self):
        # Reads the existing cache for this build.
        # If no cache exists or the cache is invalid, it will raise an error.
        cache.read_cache(self)

    def close_project(self, save_as):
        # Closes the entire project folder, including all builds.
        # Requires a valid build name to be given and saved before closing project.
        # If no builds should be saved, set save_as to None
        cache.close_project(self, save_as)

    def insert(self, row: Dict):
        # Inserts a single row into the database.
        # If the pdp was created sorted, will use that sort to insert the row
        # using a binary search (O(logN)). However, if the pdp is unsorted,
        # will jump to the end and add row (O(1)).
        operations.insert(self, row)

    def delete(self, key_or_df, single=True, key_col=None):
        # By default, deletes a single key from the df. This assumes a primary
        # key, so it will error if duplicates are found. However, if single=False,
        # duplicates are also deleted. The column to delete by is by default
        # the column the df is sorted by, or the first column if no sort is defined.
        # Can also accept a df, which would delete all matching rows from the pdp.
        # Deleting from sorted df using sorted key: O(log(N)) via binary search
        # Any other deletion requires O(n) per entry
        # Returns True if a delete occurred or False if not.
        return operations.delete(self, key_or_df, single, key_col)

    def lookup(self, key, key_col=None, save_as=None):
        # Lookup (scan) for a specific key in a similar way to delete.
        # Uses the sort by col if able (O(log N)), or scans if not (O(n)).

        # If save_as is passed, saves the df build to cache under that name.
        # Otherwise, saves it under a temp name. Returns the new pdp.
        return operations.lookup(self, key, key_col, save_as)

    def filter(self, predicate, save_as=None):
        # Filter by a given predicate. Predicate must be callable.
        # Must fully scan the db each time, so this is an O(n) operation.

        # If save_as is passed, saves the df build to cache under that name.
        # Otherwise, saves it under a temp name. Returns the new pdp.
        return operations.filter(self, predicate, save_as)

    def project(self, cols, save_as=None):
        # Project the given cols adn drop the rest of the data.
        # Can take one or multiple cols.
        # Must fully scan the db each time, so this is an O(n) operation.

        # If save_as is passed, saves the df build to cache under that name.
        # Otherwise, saves it under a temp name. Returns the new pdp.
        return operations.project(self, cols, save_as)

    def count(self, predicate):
        # Count occurences of a given predicate. Predicate must be callable.
        # Must fully scan the db each time, so this is an O(n) operation.
        # Returns the integer count.
        return operations.count(self, predicate)

    def make_snapshot(self, build_name, overwrite=False):
        # Creates a manual save of the current build. Used as a checkpoint
        # for later work to continue from. Does not return the new build.
        # Instead, work will continue on the current build.
        cache.make_snapshot(self, build_name, overwrite)

    def print(self):
        # Prints out the current state of the pdp, page by page.
        for item in self.pages:
            df = pd.read_pickle(item["path"])
            print(df)


#### HELPER FUNCTIONS ####

    def _build_pages(self):
        return build.build_pages(self)

    def _sort_df(self, df):
        return build.sort_df(self, df)

    def _write_index(self, pages):
        return cache.write_index(self, pages)

    def _insert_by_sorted_key(self, row):
        return operations.insert_by_sorted_key(self, row)

    def _page_filename(self, first, last):
        return page.page_filename(self, first, last)

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
