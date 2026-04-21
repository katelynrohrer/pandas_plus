
import os
import json
import shutil
import pandas as pd


def load_valid_index(pdp):
    if not os.path.exists(pdp.index):
        return None

    with open(pdp.index, "r") as f:
        index_data = json.load(f)

    pages = index_data["pages"]
    index_sort_by = index_data["sort_by"]
    if pdp.sort_by != index_sort_by:
        raise ValueError(f"Sorting column does not match cache. Please abort existing cache to continue with this operation\n{pdp.sort_by} != {index_sort_by}")

    if all(os.path.exists(page["path"]) for page in pages):
        print("Found cache!")
        return pages

    print("Rebuilding cache")
    return None


def write_index(pdp, pages):
    os.makedirs(pdp.page_folder, exist_ok=True)
    with open(pdp.index, "w") as f:
        json.dump({
        "sort_by": pdp.sort_by,
        "pages": pages
    }, f)


def clear_old_cache(pdp):
    for name in os.listdir(pdp.cache_root):
        path = os.path.join(pdp.cache_root, name)

        if name == pdp.page_key:
            continue

        if not name.startswith(f"{pdp.basename}_"):
            continue

        if os.path.isdir(path):
            shutil.rmtree(path)

def clear_current_cache(pdp):
    if os.path.exists(pdp.page_folder):
        shutil.rmtree(pdp.page_folder)

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
    # delete all pages in the cache, storage saver
    # use when done working on files in a session
    # -- not recommended to call on each run --
    if os.path.exists(pdp.index):
        with open(pdp.index, "r") as f:
            index_data = json.load(f)

        pages = index_data["pages"]

        for page in pages:
            path_to_remove = page["path"] if isinstance(page, dict) else page
            if os.path.exists(path_to_remove):
                os.remove(path_to_remove)

        os.remove(pdp.index)

    if os.path.exists(pdp.page_folder) and not os.listdir(pdp.page_folder):
        os.rmdir(pdp.page_folder)