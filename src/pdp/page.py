import os
import json
import hashlib
import pandas as pd


def page_is_full(pdp, df):
    return len(df) > pdp.page_row_capacity

def page_bounds(pdp, df):
    df = pdp._sort_df(df)
    first = str(df.iloc[0][pdp.columns[0]])
    last = str(df.iloc[-1][pdp.columns[0]])
    return first, last

def find_page_index(pdp, value):
    text = "" if pd.isna(value) else str(value)

    for i, page in enumerate(pdp.pages):
        if page["first"] == "" and page["last"] == "":
            continue
        if page["first"] <= text <= page["last"]:
            return i

    first_char = text[:1].upper()

    for i, page in enumerate(pdp.pages):
        if page["first"] == "" and page["last"] == "":
            page_name = os.path.basename(page["path"])
            if page_name.startswith(f"page_{first_char}"):
                return i

    for i, page in enumerate(pdp.pages):
        if page["first"] == "" and page["last"] == "":
            page_name = os.path.basename(page["path"])
            if page_name.startswith("page_"):
                return i

    for i, page in enumerate(pdp.pages):
        if text < page["first"]:
            return i

    return len(pdp.pages) - 1


def page_filename(pdp, df):
    first_row = "|".join(str(v) for v in df.iloc[0].tolist())
    last_row = "|".join(str(v) for v in df.iloc[-1].tolist())

    first_hash = hashlib.sha1(first_row.encode()).hexdigest()[:8]
    last_hash = hashlib.sha1(last_row.encode()).hexdigest()[:8]

    return os.path.join(pdp.page_folder, f"{first_hash}__{last_hash}.pickle")


def write_page(pdp, df, idx=None):
    df = pdp._sort_df(df)

    if df.empty:
        filename = os.path.join(pdp.page_folder, f"page_{idx if idx is not None else 'empty'}.pickle")
        pd.to_pickle(df, filename)
        return {"path": filename, "first": "", "last": ""}

    first, last = pdp._page_bounds(df)
    filename = pdp._page_filename(df)
    pd.to_pickle(df, filename)
    return {"path": filename, "first": first, "last": last}


def rewrite_page(pdp, idx, df):
    old_path = pdp.pages[idx]["path"]

    if df.empty:
        if os.path.exists(old_path):
            os.remove(old_path)
        del pdp.pages[idx]
        pdp._write_index(pdp.pages)
        return

    new_page = pdp._write_page(df)
    pdp.pages[idx] = new_page

    if old_path != new_page["path"] and os.path.exists(old_path):
        os.remove(old_path)


def split_page(pdp, idx, df):
    col = pdp.columns[0]
    df = pdp._sort_df(df).reset_index(drop=True)

    mid = len(df) // 2
    left = df.iloc[:mid].reset_index(drop=True)
    right = df.iloc[mid:].reset_index(drop=True)
    old_path = pdp.pages[idx]["path"]

    while not left.empty and not right.empty and left.iloc[-1][col] == right.iloc[0][col]:
        dup_value = right.iloc[0][col]
        dup_rows = right[right[col] == dup_value].reset_index(drop=True)
        right = right[right[col] != dup_value].reset_index(drop=True)
        left_last = left.iloc[-1][col] if not left.empty else None

        if left_last == dup_value:
            center = dup_rows

            left_page = pdp._write_page(left)
            center_page = pdp._write_page(center)
            right_page = pdp._write_page(right) if not right.empty else None

            pdp.pages[idx] = left_page
            inserts = [center_page]

            if right_page is not None:
                inserts.append(right_page)

            pdp.pages[idx + 1:idx + 1] = inserts

            if old_path != left_page["path"] and os.path.exists(old_path):
                os.remove(old_path)

            pdp._remove_empty_pages()
            return

    left_page = pdp._write_page(left)
    right_page = pdp._write_page(right)

    pdp.pages[idx] = left_page
    pdp.pages.insert(idx + 1, right_page)

    if old_path != left_page["path"] and os.path.exists(old_path):
        os.remove(old_path)

    pdp._remove_empty_pages()


def remove_empty_pages(pdp):
    new_pages = []

    for page in pdp.pages:
        df = pd.read_pickle(page["path"])
        if df.empty:
            if os.path.exists(page["path"]):
                os.remove(page["path"])
            continue
        new_pages.append(page)

    pdp.pages = new_pages
    pdp._write_index(pdp.pages)


def write_index(pdp, pages):
    with open(pdp.index, "w") as f:
        json.dump(pages, f)


def load_page(pdp, idx):
    return pd.read_pickle(pdp.pages[idx]["path"])

