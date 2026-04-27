import gc
import os
import pandas as pd


def set_sort_mode(pdp, sort, sort_col):
    if not sort:
        pdp.sort_by = None
        return

    if sort_col:
        if sort_col not in pdp.columns:
            raise ValueError(f"column to be sorted on must exist within df.\ncurrent columns:\n{pdp.columns}")
        pdp.columns.remove(sort_col)
        pdp.columns.insert(0, sort_col)

    pdp.sort_by = pdp.columns[0]

def build_pages(pdp):
    if pdp.sort_by is not None and pdp.columns[0] != pdp.sort_by:
        pdp.columns.remove(pdp.sort_by)
        pdp.columns.insert(0, pdp.sort_by)
    if pdp.can_fit_in_mem:
        df = pd.read_csv(pdp.file, dtype=str)
        df = df[pdp.columns]
        pages_from_df(pdp, df)
        return
    if pdp.sort_by is None:
        build_pages_unsorted(pdp)
        return
    attempt = 0
    try:
        build_pages_sorted(pdp)
    except MemoryError:
        print("Ran out of memory while building index. Retrying with smaller chunks.")
        pdp._write_index(pdp.pages, complete=False)
        attempt += 1
        build_pages_sorted(pdp, attempt)


def build_pages_unsorted(pdp):
    pages = []
    print("starting unsorted paged build...")

    for chunk_num, chunk in enumerate(pd.read_csv(pdp.file, dtype=str, chunksize=pdp.page_row_capacity)):
        chunk = chunk[pdp.columns]
        print(f"processing chunk {chunk_num}...")

        chunk = chunk.reset_index(drop=True)

        for i in range(0, len(chunk), pdp.page_row_capacity):
            page_df = chunk.iloc[i:i + pdp.page_row_capacity].reset_index(drop=True)
            pages.append(pdp._write_page(page_df, len(pages)))

    if not pages:
        empty_df = pd.DataFrame(columns=pdp.columns)
        pages.append(pdp._write_page(empty_df, 0))

    pdp.pages = pages
    pdp._write_index(pdp.pages)

def build_pages_sorted(pdp, attempt=1):
    # initializing empty first page
    print(pdp.columns)
    first = chr(0)
    last = chr(0x10FFFF)
    filename = pdp._page_filename(first, last)
    os.makedirs(pdp.page_folder, exist_ok=True)

    page = {"path": filename, "first": first, "last": last}
    pd.to_pickle(pd.DataFrame(columns=pdp.columns), filename)

    pdp.pages = [page]
    pdp._write_index(pdp.pages)

    chunk_size = pdp.page_row_capacity // attempt

    for chunk_num, chunk in enumerate(pd.read_csv(pdp.file, dtype=str, chunksize=chunk_size)):
        chunk = chunk[pdp.columns]
        updates = {}
        print(f"processing chunk {chunk_num}...")

        sort_values = chunk[pdp.sort_by].astype(str).tolist()
        for row_i, row_value in enumerate(sort_values):
            page_idx = pdp._find_page_index_binary(row_value)
            updates.setdefault(page_idx, []).append(row_i)

        offset = 0
        for original_idx in sorted(updates.keys()):
            page_idx = original_idx + offset
            page_df = pdp._load_page(page_idx)
            new_rows_df = chunk.iloc[updates[original_idx]].reset_index(drop=True)
            # adding them all then sorting is faster than insertion sort on build
            combined_df = pd.concat([page_df, new_rows_df], ignore_index=True)

            del page_df
            del new_rows_df

            combined_df = sort_df(pdp, combined_df)
            pdp._update_page(page_idx, combined_df)

            while page_idx < len(pdp.pages):
                current_df = pdp._load_page(page_idx)
                if not pdp._page_is_full(current_df):
                    del current_df
                    break

                before = len(pdp.pages)
                pdp._split_page(page_idx, current_df)
                after = len(pdp.pages)
                offset += after - before
                del current_df

            del combined_df

        # explicit trash collection helps open memory up
        del updates
        del sort_values
        del chunk

        if "page_df" in locals():
            del page_df
        if "new_rows_df" in locals():
            del new_rows_df
        if "combined_df" in locals():
            del combined_df
        if "current_df" in locals():
            del current_df

        gc.collect()

        pdp._write_index(pdp.pages)

    return pdp.pages


def pages_from_df(pdp, df):
    df = sort_df(pdp, df)

    pages = []
    if df.empty:
        pages.append(pdp._write_page(df, 0))
        return pages

    for i in range(0, len(df), pdp.page_row_capacity):
        page_df = df.iloc[i:i + pdp.page_row_capacity].reset_index(drop=True)
        pages.append(pdp._write_page(page_df, i // pdp.page_row_capacity))

    pdp.pages = pages

# move out of here if used later in non-initial build
def sort_df(pdp, df):
    if df.empty:
        return df.reset_index(drop=True)

    if pdp.sort_by is None:
        return df.reset_index(drop=True) # todo why?

    return df.sort_values(by=pdp.sort_by, kind="stable").reset_index(drop=True)