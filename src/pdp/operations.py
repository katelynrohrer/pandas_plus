
import os
import pandas as pd
from typing import Dict
from bisect import bisect_right


### Insert functions ###

def insert(pdp, row: Dict):
    if set(row.keys()) != set(pdp.columns):
        raise KeyError("New row must have the same columns as the rest of the df")

    if pdp.sort_by is None:
        return insert_by_scan(pdp, row)

    return insert_by_sorted_key(pdp, row)


def insert_by_scan(pdp, row: Dict):
    if not pdp.pages:
        new_page = pdp._write_page(pd.DataFrame([row], columns=pdp.columns))
        pdp.pages.append(new_page)
        pdp._write_index(pdp.pages)
        return

    page_idx = len(pdp.pages) - 1
    page_df = pdp._load_page(page_idx)

    if pdp._page_is_full(page_df):
        print("full page detected!")
        new_page = pdp._write_page(pd.DataFrame([row], columns=pdp.columns))
        pdp.pages.append(new_page)
    else:
        page_df = concat_insert(pdp, page_df, row)
        pdp._update_page(page_idx, page_df)
        update_page_index(pdp, page_idx, page_df)

    pdp._write_index(pdp.pages)


def insert_by_sorted_key(pdp, row: Dict):
    row_value = str(row[pdp.sort_by])

    if not pdp.pages:
        new_page = pdp._write_page(pd.DataFrame([row], columns=pdp.columns))
        pdp.pages.append(new_page)
        pdp._write_index(pdp.pages)
        return

    page_idx = pdp._find_page_index_binary(row_value)
    page_df = pdp._load_page(page_idx)
    page_df = insertion_sort_insert(pdp, page_df, row)

    if pdp._page_is_full(page_df):
        print("full page detected!")
        pdp._split_page(page_idx, page_df)
    else:
        pdp._update_page(page_idx, page_df)
        update_page_index(pdp, page_idx, page_df)

    pdp._write_index(pdp.pages)


def concat_insert(pdp, df, row):
    new_row_df = pd.DataFrame([row], columns=pdp.columns)
    return  pd.concat([df, new_row_df], ignore_index=True)


def insertion_sort_insert(pdp, df, row):
    if pdp.sort_by is None:
        raise ValueError("sorted insert requires pdp.sort_by to be set")

    values = df[pdp.sort_by].astype(str).tolist()
    insert_at = bisect_right(values, str(row[pdp.sort_by]))

    top = df.iloc[:insert_at]
    bottom = df.iloc[insert_at:]
    new_row_df = pd.DataFrame([row], columns=pdp.columns)
    return pd.concat([top, new_row_df, bottom], ignore_index=True)


### Delete functions ###

def delete(pdp, key, single=True, key_col=None):
    key = str(key)
    if key_col is None:
        key_col = pdp.sort_by if pdp.sort_by is not None else pdp.columns[0]
    elif key_col not in pdp.columns:
        raise ValueError("delete key column not found in df")

    if pdp.sort_by is not None and key_col == pdp.sort_by:
        return delete_by_sorted_key(pdp, key, single)

    return delete_by_scan(pdp, key, key_col, single)


def delete_by_sorted_key(pdp, key, single=True):
    page_idx = pdp._find_page_index_binary(key)
    if page_idx is None:
        return False

    page_df = pdp._load_page(page_idx)
    page_df[pdp.sort_by] = page_df[pdp.sort_by].astype(str)

    matches = page_df[page_df[pdp.sort_by] == key]

    if len(matches) == 0:
        return False

    if single and len(matches) > 1:
        raise ValueError(f"duplicate key found during delete: {key}")

    page_df = page_df[page_df[pdp.sort_by] != key].reset_index(drop=True)

    if page_df.empty:
        old_path = pdp.pages[page_idx]["path"]
        if os.path.exists(old_path):
            os.remove(old_path)
        del pdp.pages[page_idx]
        pdp._write_index(pdp.pages)
        return True

    pdp._update_page(page_idx, page_df)
    update_page_index(pdp, page_idx, page_df)
    pdp._write_index(pdp.pages)
    return True


def delete_by_scan(pdp, key, key_col, single=True):
    total_matches = 0
    matches_by_page = []

    for page_idx in range(len(pdp.pages)):
        page_df = pdp._load_page(page_idx)
        page_df[key_col] = page_df[key_col].astype(str)
        matches = page_df[page_df[key_col] == key]

        if len(matches) > 0:
            total_matches += len(matches)
            matches_by_page.append((page_idx, page_df, matches))

    if total_matches == 0:
        return False

    if single and total_matches > 1:
        raise ValueError(f"duplicate key found during delete: {key}")

    pages_to_delete = []

    for page_idx, page_df, matches in matches_by_page:
        page_df = page_df[page_df[key_col] != key].reset_index(drop=True)

        if page_df.empty:
            old_path = pdp.pages[page_idx]["path"]
            if os.path.exists(old_path):
                os.remove(old_path)
            pages_to_delete.append(page_idx)
        else:
            pdp._update_page(page_idx, page_df)
            update_page_index(pdp, page_idx, page_df)

    for page_idx in reversed(pages_to_delete):
        del pdp.pages[page_idx]

    pdp._write_index(pdp.pages)
    return True


### Lookup functions ###

def lookup(pdp, key, key_col=None):
    key = str(key)
    if key_col is None:
        key_col = pdp.sort_by if pdp.sort_by is not None else pdp.columns[0]
    elif key_col not in pdp.columns:
        raise ValueError("lookup key column not found in df")

    if pdp.sort_by is not None and key_col == pdp.sort_by:
        return lookup_by_sorted_key(pdp, key)

    return lookup_by_scan(pdp, key, key_col)


def lookup_by_sorted_key(pdp, key):
    page_idx = pdp._find_page_index_binary(key)
    if page_idx is None:
        return pd.DataFrame(columns=pdp.columns)

    page_df = pdp._load_page(page_idx)
    page_df[pdp.sort_by] = page_df[pdp.sort_by].astype(str)

    matches = page_df[page_df[pdp.sort_by] == key].reset_index(drop=True)

    return matches.reset_index(drop=True)


def lookup_by_scan(pdp, key, key_col):
    matches_by_page = []
    total_matches = 0

    for page_idx in range(len(pdp.pages)):
        page_df = pdp._load_page(page_idx)
        page_df[key_col] = page_df[key_col].astype(str)
        matches = page_df[page_df[key_col] == key]

        if len(matches) > 0:
            total_matches += len(matches)
            matches_by_page.append(matches)

    if total_matches == 0:
        return pd.DataFrame(columns=pdp.columns)

    return pd.concat(matches_by_page, ignore_index=True)


### Helpers ###

def update_page_index(pdp, page_idx, page_df):
    if pdp.sort_by is None:
        pdp.pages[page_idx]["first"] = ""
        pdp.pages[page_idx]["last"] = ""
        return

    pdp.pages[page_idx]["first"] = str(page_df.iloc[0][pdp.sort_by])
    pdp.pages[page_idx]["last"] = str(page_df.iloc[-1][pdp.sort_by])
