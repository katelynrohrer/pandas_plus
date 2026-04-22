
import os
import json
import shutil
import pandas as pd


def write_index(pdp, pages):
    os.makedirs(pdp.page_folder, exist_ok=True)
    with open(pdp.index, "w") as f:
        json.dump({
        "sort_by": pdp.sort_by,
        "pages": pages
    }, f)

def build_cache(pdp):
    if pdp.cache_exists():
        raise FileExistsError("cache already exists. please abort the previous cache to continue.")

    pdp._build_pages()
    pdp._write_index(pdp.pages)
    return

def commit_cache(pdp):
    first_page = True
    for page in pdp.pages:
        df = pd.read_pickle(page["path"])
        df.to_csv(
            pdp.file,
            mode="w" if first_page else "a",
            index=False,
            header=first_page,
        )
        first_page = False

def abort_cache(pdp):
    if os.path.exists(pdp.page_folder):
        shutil.rmtree(pdp.page_folder)

def read_cache(pdp):
    if pdp.cache_is_valid():  # which also checks that it exists
        with open(pdp.index, "r") as f:
            index_data = json.load(f)

        pdp.sort_by = index_data["sort_by"]
        pdp.pages = index_data["pages"]
        return

    if pdp.cache_exists():
        raise FileExistsError("invalid cache exists. please abort the previous cache to continue.")

def cache_is_valid(pdp):
    if not pdp.cache_exists():
        return False

    with open(pdp.index, "r") as f:
        index_data = json.load(f)

    pages = index_data["pages"]
    index_sort_by = index_data["sort_by"]
    if pdp.sort_by != index_sort_by:
        print(f"Sorting column does not match cache. Please abort existing cache to continue with this operation\n{pdp.sort_by} != {index_sort_by}")
        return False

    if all(os.path.exists(item["path"]) for item in pages):
        return True
    return False