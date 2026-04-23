import os
import pandas as pd
from typing import Dict
from bisect import bisect_right


### Insert functions ###

def insert(pdp, row: Dict):
    if set(row.keys()) != set(pdp.columns):
        raise KeyError("New row must have the same columns as the rest of the df")
    elif pdp.sort_by is None:
        insert_by_scan(pdp, row)
    else:
        insert_by_sorted_key(pdp, row)


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
    return pd.concat([df, new_row_df], ignore_index=True)


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

def delete(pdp, key, allow_duplicates=False, key_col=None):
    if isinstance(key, pd.DataFrame):
        return delete_by_df(pdp, key, allow_duplicates)

    key = str(key)
    if key_col is None:
        key_col = pdp.sort_by if pdp.sort_by is not None else pdp.columns[0]
    elif key_col not in pdp.columns:
        raise ValueError("delete key column not found in df")

    if pdp.sort_by is not None and key_col == pdp.sort_by:
        return delete_by_sorted_key(pdp, key, allow_duplicates)

    return delete_by_scan(pdp, key, key_col, allow_duplicates)


def delete_by_sorted_key(pdp, key, allow_duplicates=False):
    candidate_pages = []

    page_idx = pdp._find_page_index_binary(key)
    if page_idx is not None:
        candidate_pages.append(page_idx)

    if page_idx is not None and page_idx - 1 >= 0:
        prev_page = pdp.pages[page_idx - 1]
        if str(prev_page["first"]) <= key <= str(prev_page["last"]):
            candidate_pages.append(page_idx - 1)

    if page_idx is not None and page_idx + 1 < len(pdp.pages):
        next_page = pdp.pages[page_idx + 1]
        if str(next_page["first"]) <= key <= str(next_page["last"]):
            candidate_pages.append(page_idx + 1)

    candidate_pages = sorted(set(candidate_pages))
    if not candidate_pages:
        return False

    total_matches = 0
    matches_by_page = []

    for candidate_idx in candidate_pages:
        page_df = pdp._load_page(candidate_idx)
        page_df[pdp.sort_by] = page_df[pdp.sort_by].astype(str)
        matches = page_df[page_df[pdp.sort_by] == key]

        if len(matches) > 0:
            total_matches += len(matches)
            matches_by_page.append((candidate_idx, page_df))

    if total_matches == 0:
        return False

    if not allow_duplicates and total_matches > 1:
        raise ValueError(f"duplicate key found during delete: {key}")

    pages_to_delete = []

    for candidate_idx, page_df in matches_by_page:
        page_df = page_df[page_df[pdp.sort_by] != key].reset_index(drop=True)

        if page_df.empty:
            old_path = pdp.pages[candidate_idx]["path"]
            if os.path.exists(old_path):
                os.remove(old_path)
            pages_to_delete.append(candidate_idx)
        else:
            pdp._update_page(candidate_idx, page_df)
            update_page_index(pdp, candidate_idx, page_df)

    for candidate_idx in reversed(pages_to_delete):
        del pdp.pages[candidate_idx]

    pdp._write_index(pdp.pages)
    return True


def delete_by_scan(pdp, key, key_col, allow_duplicates=False):
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

    if not allow_duplicates and total_matches > 1:
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


def delete_by_df(pdp, df, allow_duplicates=False):
    if df.empty:
        return False

    rows = df.to_dict(orient="records")
    deleted_any = False

    for row in rows:
        row_matches = 0

        for page_idx in range(len(pdp.pages)):
            page_df = pdp._load_page(page_idx)
            if page_df.empty:
                continue

            page_matches = pd.Series(True, index=page_df.index)
            for col in pdp.columns:
                page_matches &= page_df[col].astype(str) == str(row[col])

            row_matches += int(page_matches.sum())

        if row_matches == 0:
            continue

        if not allow_duplicates and row_matches > 1:
            raise ValueError(f"duplicate row found during delete: {row}")

        pages_to_delete = []

        for page_idx in range(len(pdp.pages)):
            page_df = pdp._load_page(page_idx)
            if page_df.empty:
                continue

            page_matches = pd.Series(True, index=page_df.index)
            for col in pdp.columns:
                page_matches &= page_df[col].astype(str) == str(row[col])

            if not page_matches.any():
                continue

            page_df = page_df[~page_matches].reset_index(drop=True)

            if page_df.empty:
                old_path = pdp.pages[page_idx]["path"]
                if os.path.exists(old_path):
                    os.remove(old_path)
                pages_to_delete.append(page_idx)
            else:
                pdp._update_page(page_idx, page_df)
                update_page_index(pdp, page_idx, page_df)

            deleted_any = True

            if not allow_duplicates:
                break

        for page_idx in reversed(pages_to_delete):
            del pdp.pages[page_idx]

    pdp._write_index(pdp.pages)
    return deleted_any


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
    candidate_pages = []

    page_idx = pdp._find_page_index_binary(key)
    if page_idx is not None:
        candidate_pages.append(page_idx)

    # looking at the page before
    if page_idx is not None and page_idx - 1 >= 0:
        prev_page = pdp.pages[page_idx - 1]
        if str(prev_page["first"]) <= key <= str(prev_page["last"]):
            candidate_pages.append(page_idx - 1)

    # looking at the page after
    if page_idx is not None and page_idx + 1 < len(pdp.pages):
        next_page = pdp.pages[page_idx + 1]
        if str(next_page["first"]) <= key <= str(next_page["last"]):
            candidate_pages.append(page_idx + 1)

    candidate_pages = sorted(set(candidate_pages))
    if not candidate_pages:
        return pd.DataFrame(columns=pdp.columns)

    matches_by_page = []
    total_matches = 0

    # looking at all valid nearby pages
    for candidate_idx in candidate_pages:
        page_df = pdp._load_page(candidate_idx)
        page_df[pdp.sort_by] = page_df[pdp.sort_by].astype(str)
        matches = page_df[page_df[pdp.sort_by] == key]

        if len(matches) > 0:
            total_matches += len(matches)
            matches_by_page.append(matches)

    if total_matches == 0:
        return pd.DataFrame(columns=pdp.columns)

    return pd.concat(matches_by_page, ignore_index=True)


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


### Filter ###

def filter(pdp, predicate, save_as=None):
    if not callable(predicate):
        raise TypeError("filter expects a callable (e.g., lambda x: ...)")

    return materialize_derived_build(
        pdp = pdp,
        save_as = save_as,
        temp_suffix = "filter_tmp",
        sort_by = pdp.sort_by,
        columns = pdp.columns,
        transform_page = lambda page_df: page_df[page_df.apply(predicate, axis=1)],
    )


### Project ###

def project(pdp, cols, save_as=None):
    if isinstance(cols, str):
        cols = [cols]

    if not cols:
        raise ValueError("project requires at least one column")

    missing = []
    for col in cols:
        if col not in pdp.columns:
            missing.append(col)
    if missing:
        raise ValueError(f"project columns not found in df: {missing}")

    return materialize_derived_build(
        pdp = pdp,
        save_as = save_as,
        temp_suffix = "project_tmp",
        sort_by = None,
        columns = cols,
        transform_page = lambda page_df: page_df[cols].reset_index(drop=True),
    )


### Count ###

def count(pdp, predicate=None):
    if predicate is not None and not callable(predicate):
        raise TypeError("count expects predicate to be callable or None")

    total = 0

    for page_idx in range(len(pdp.pages)):
        page_df = pdp._load_page(page_idx)
        if page_df.empty:
            continue

        if predicate is None:
            total += len(page_df)
        else:
            matches = page_df.apply(predicate, axis=1)
            total += int(matches.astype(bool).sum())

    return total


### Helpers ###

# Used for filter and project to save the build under a new name while it's being created
def materialize_derived_build(pdp, save_as, temp_suffix, sort_by, columns, transform_page):
    if save_as is None:
        if not hasattr(pdp, "_temp_build_counter"):
            pdp._temp_build_counter = 0
        pdp._temp_build_counter += 1
        base_name = getattr(pdp, "build_name", "build")
        save_as = f"{base_name}__{temp_suffix}_{pdp._temp_build_counter}"

    if not isinstance(save_as, str) or not save_as.strip():
        raise ValueError("save_as must be a non-empty string")

    new_pdp = pdp.__class__(pdp.file, sort_col=sort_by, build_name=save_as)
    new_pdp.pages = []
    new_pdp.columns = columns

    current_rows = []

    for idx in range(len(pdp.pages)):
        page_df = pdp._load_page(idx)
        if page_df.empty:
            continue

        transformed = transform_page(page_df)
        if transformed.empty:
            continue

        transformed_rows = transformed.to_dict(orient="records")
        for row in transformed_rows:
            current_rows.append(row)

            if len(current_rows) == new_pdp.page_row_capacity:
                out_df = pd.DataFrame(current_rows, columns=new_pdp.columns)
                new_page = new_pdp._write_page(out_df)
                new_pdp.pages.append(new_page)
                current_rows = []

    if current_rows:
        out_df = pd.DataFrame(current_rows, columns=new_pdp.columns)
        new_page = new_pdp._write_page(out_df)
        new_pdp.pages.append(new_page)

    new_pdp._write_index(new_pdp.pages)
    return new_pdp

# used to update the index
def update_page_index(pdp, page_idx, page_df):
    if pdp.sort_by is None:
        pdp.pages[page_idx]["first"] = ""
        pdp.pages[page_idx]["last"] = ""
        return

    pdp.pages[page_idx]["first"] = str(page_df.iloc[0][pdp.sort_by])
    pdp.pages[page_idx]["last"] = str(page_df.iloc[-1][pdp.sort_by])
