
import os
import json
import shutil
import pandas as pd
import hashlib


def write_index(pdp, pages):
    os.makedirs(pdp.page_folder, exist_ok=True)
    with open(pdp.index, "w") as f:
        pages_with_hash = []
        for page in pages:
            page_hash = hash_file(page["path"]) if os.path.exists(page["path"]) else None
            new_page = dict(page)
            new_page["hash"] = page_hash
            pages_with_hash.append(new_page)
        json.dump({
            "build_name": pdp.build_name,
            "sort_by": pdp.sort_by,
            "columns": pdp.columns,
            "pages": pages_with_hash
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


# Save the current cache under a new build name and folder.
def make_snapshot(pdp, build_name, overwrite=False):
    new_page_folder = os.path.join(pdp.build_root, build_name)
    new_index = os.path.join(new_page_folder, "index.json")
    new_meta = os.path.join(new_page_folder, "meta.json")

    if os.path.exists(new_page_folder) and not overwrite:
        raise FileExistsError("cache already exists for this build name. please choose another name or overwrite the existing cache.")
    if os.path.exists(new_page_folder) and overwrite:
        shutil.rmtree(new_page_folder)

    os.makedirs(new_page_folder, exist_ok=False)

    new_pages = []
    for idx, page in enumerate(pdp.pages):
        df = pd.read_pickle(page["path"])
        filename = os.path.basename(page["path"])
        new_path = os.path.join(new_page_folder, filename)
        pd.to_pickle(df, new_path)

        new_page = dict(page)
        new_page["path"] = new_path
        new_pages.append(new_page)

    # write just needs these fields to be set correctly. this is a workaround
    temp_build_name, pdp.build_name = pdp.build_name, build_name
    temp_page_folder, pdp.page_folder = pdp.page_folder, new_page_folder
    temp_index, pdp.index = pdp.index, new_index
    temp_meta, pdp.meta = pdp.meta, new_meta

    write_index(pdp, new_pages)

    pdp.build_name, temp_build_name = temp_build_name, pdp.build_name
    pdp.page_folder, temp_page_folder = temp_page_folder, pdp.page_folder
    pdp.index, temp_index = temp_index, pdp.index
    pdp.meta, temp_meta = temp_meta, pdp.meta

def abort_cache(pdp):
    if os.path.exists(pdp.page_folder):
        shutil.rmtree(pdp.page_folder)

def read_cache(pdp, overwrite=False):
    if pdp.cache_is_valid():  # which also checks that it exists
        with open(pdp.index, "r") as f:
            index_data = json.load(f)

        pdp.build_name = index_data["build_name"]
        pdp.sort_by = index_data["sort_by"]
        pdp.columns = index_data["columns"]
        pdp.pages = index_data["pages"]
        return

    if pdp.cache_exists() and not overwrite:
        raise FileExistsError("invalid cache exists. please abort the previous cache or set overwrite=True to continue.")
    if pdp.cache_exists() and overwrite:
        abort_cache(pdp)
        read_cache(pdp)

def cache_is_valid(pdp):
    if not pdp.cache_exists():
        return False

    with open(pdp.index, "r") as f:
        index_data = json.load(f)

    pages = index_data["pages"]
    index_build_name = index_data["build_name"]
    index_sort_by = index_data["sort_by"]
    index_columns = index_data["columns"]

    for page in pages:
        path = page["path"]
        expected_hash = page["hash"]
        if not os.path.exists(path):
            return False

        current_hash = hash_file(path)
        if expected_hash != current_hash:
            print(f"Page modified: {path}")
            return False

    if pdp.build_name != index_build_name:
        print(f"Build name does not match cache. Please abort existing cache to continue with this operation\n{pdp.build_name} != {index_build_name}")
        return False

    if pdp.columns != index_columns:
        print(f"Columns do not match cache. Please abort existing cache to continue with this operation\n{pdp.columns} != {index_columns}")
        return False

    if pdp.sort_by != index_sort_by:
        print(f"Sorting column does not match cache. Please abort existing cache to continue with this operation\n{pdp.sort_by} != {index_sort_by}")
        return False

    return True

def close_project(pdp, save_as):
    if save_as is not None and not isinstance(save_as, str):
        raise ValueError("Please define a build to save before closing project, or pass None to close without saving.")

    if isinstance(save_as, str):
        build_path = os.path.join(pdp.build_root, save_as)
        if not os.path.exists(build_path):
            raise NameError("Build does not exist. Please check the name and try again.")

        current_build_name = pdp.build_name
        current_page_folder = pdp.page_folder
        current_index = pdp.index
        current_meta = pdp.meta
        current_pages = pdp.pages

        pdp.build_name = save_as
        pdp.page_folder = build_path
        pdp.index = os.path.join(build_path, "index.json")
        pdp.meta = os.path.join(build_path, "meta.json")

        pdp._read_cache()
        pdp._commit_cache()

        pdp.build_name = current_build_name
        pdp.page_folder = current_page_folder
        pdp.index = current_index
        pdp.meta = current_meta
        pdp.pages = current_pages

    if os.path.exists(pdp.build_root):
        shutil.rmtree(pdp.build_root)

# Helper function to compute the hash of a file
def hash_file(path):
    hasher = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            hasher.update(chunk)
    return hasher.hexdigest()
