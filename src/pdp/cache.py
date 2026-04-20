import os
import json


def write_index(pdp, pages):
    with open(pdp.index, "w") as f:
        json.dump(pages, f)


def clear_old_cache(pdp):
    for name in os.listdir(pdp.cache_root):
        path = os.path.join(pdp.cache_root, name)

        if name == pdp.page_key:
            continue

        if not name.startswith(f"{pdp.basename}_"):
            continue

        if os.path.isdir(path):
            index = os.path.join(path, "index.json")

            if os.path.exists(index):
                with open(index, "r") as f:
                    pages = json.load(f)

                for page in pages:
                    path_to_remove = page["path"] if isinstance(page, dict) else page
                    if os.path.exists(path_to_remove):
                        os.remove(path_to_remove)

                os.remove(index)

            if not os.listdir(path):
                os.rmdir(path)


def abort_cache(pdp):
    # delete all pages in the cache, storage saver
    # use when done working on files in a session
    # -- not recommended to call on each run --
    if os.path.exists(pdp.index):
        with open(pdp.index, "r") as f:
            pages = json.load(f)

        for page in pages:
            path_to_remove = page["path"] if isinstance(page, dict) else page
            if os.path.exists(path_to_remove):
                os.remove(path_to_remove)

        os.remove(pdp.index)

    if os.path.exists(pdp.page_folder) and not os.listdir(pdp.page_folder):
        os.rmdir(pdp.page_folder)


def load_valid_index(pdp):
    if not os.path.exists(pdp.index):
        return None

    with open(pdp.index, "r") as f:
        pages = json.load(f)

    if all(os.path.exists(page["path"]) for page in pages):
        print("Found cache!")
        return pages

    print("Rebuilding cache")
    return None


def clear_current_cache(pdp):
    if not os.path.exists(pdp.page_folder):
        return

    if os.path.exists(pdp.index):
        with open(pdp.index, "r") as f:
            pages = json.load(f)

        for page in pages:
            path_to_remove = page["path"] if isinstance(page, dict) else page
            if os.path.exists(path_to_remove):
                os.remove(path_to_remove)

        os.remove(pdp.index)

    for name in os.listdir(pdp.page_folder):
        path = os.path.join(pdp.page_folder, name)

        if os.path.isfile(path):
            os.remove(path)
        elif os.path.isdir(path):
            for root, dirs, files in os.walk(path, topdown=False):
                for file in files:
                    os.remove(os.path.join(root, file))
                for directory in dirs:
                    os.rmdir(os.path.join(root, directory))
            os.rmdir(path)