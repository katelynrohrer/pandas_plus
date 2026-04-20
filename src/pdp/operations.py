
import os
from typing import Dict
from bisect import bisect_right

def insert(pdp, row: Dict):
    if set(row.keys()) != set(pdp.columns):
        raise KeyError("New row must have the same columns as the rest of the df")

    row_value = row[pdp.columns[0]]

    if not pdp.chunks:
        new_page = pdp._write_page(pd.DataFrame([row], columns=pdp.columns))
        pdp.chunks.append(new_page)
        pdp._write_index(pdp.chunks)
        return

    page_idx = pdp._find_page_index(row_value)
    page_df = pdp._load_page(page_idx)
    page_df = pdp._insert_sorted_row(page_df, row, )

    if pdp._page_is_full(page_df):
        print("full page detected!")
        pdp._split_page(page_idx, page_df)
    else:
        pdp._rewrite_page(page_idx, page_df)

    pdp._write_index(pdp.chunks)

def delete(pdp, key):
    key = str(key)
    key_col = pdp.columns[0]

    page_idx = pdp._find_page_idx(key)
    if page_idx is None:
        return False

    page_df = pdp._load_page(page_idx)
    page_df[key_col] = page_df[key_col].astype(str)

    matches = page_df[page_df[key_col] == key]

    if len(matches) == 0:
        return False

    if len(matches) > 1:
        raise ValueError(f"duplicate key found during delete: {key}")

    page_df = page_df[page_df[key_col] != key].reset_index(drop=True)

    if page_df.empty:
        old_path = pdp.pages[page_idx]["path"]
        if os.path.exists(old_path):
            os.remove(old_path)
        del pdp.pages[page_idx]
        return True

    pdp._rewrite_page(page_idx, page_df)
    pdp._refresh_page_index(page_idx, page_df, )
    return True

def _refresh_page_index(pdp, page_idx, page_df):
    key_col = pdp.columns[0]
    pdp.pages[page_idx]["first"] = str(page_df.iloc[0][key_col])
    pdp.pages[page_idx]["last"] = str(page_df.iloc[-1][key_col])

def _insert_sorted_row(pdp, df, row):
    col = pdp.columns[0]
    values = df[col].tolist()
    insert_at = bisect_right(values, row[col])

    top = df.iloc[:insert_at]
    bottom = df.iloc[insert_at:]
    new_row_df = pd.DataFrame([row], columns=pdp.columns)
    return pd.concat([top, new_row_df, bottom], ignore_index=True)
