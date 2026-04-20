
import pandas as pd


def build_pages(pdp):
    if pdp.can_fit_in_mem:
        df = pd.read_csv(pdp.file, dtype=str)
        return pdp._pages_from_df(df)
    return pages_from_buckets(pdp)


def pages_from_buckets(pdp):
    pages = []

    for letter in "ABCDEFGHIJKLMNOPQRSTUVWXYZ":
        empty_df = pd.DataFrame(columns=pdp.columns)
        page = pdp._write_page(empty_df, idx=letter)
        pages.append(page)

    pdp.chunks = pages
    pdp._write_index(pdp.chunks)

    key_col = pdp.columns[0]
    chunk_num = 0
    print("starting paged build...")

    # use build in generator for chunking
    for chunk in pd.read_csv(pdp.file, dtype=str, chunksize=pdp.page_row_capacity):
        chunk_num += 1
        print(f"processing chunk {chunk_num}...")
        updates = {}

        for _, row in chunk.iterrows():
            row_dict = row.to_dict()
            row_value = row_dict.get(key_col, "")
            page_idx = find_page_index_binary(pdp, row_value)
            updates.setdefault(page_idx, []).append(row_dict)

        touched_pages = []

        for page_idx in sorted(updates.keys()):
            page_df = pdp._load_page(page_idx)
            new_rows_df = pd.DataFrame(updates[page_idx], columns=pdp.columns)
            page_df = pd.concat([page_df, new_rows_df], ignore_index=True)
            page_df = pdp._sort_df(page_df)
            pdp._rewrite_page(page_idx, page_df)
            touched_pages.append(page_idx)

        offset = 0
        for original_idx in touched_pages:
            page_idx = original_idx + offset

            while page_idx < len(pdp.chunks):
                current_df = pdp._load_page(page_idx)
                if not pdp._page_is_full(current_df):
                    break

                before = len(pdp.chunks)
                pdp._split_page(page_idx, current_df)
                after = len(pdp.chunks)
                offset += after - before

    pdp._remove_empty_pages()
    pdp._write_index(pdp.chunks)
    return pdp.chunks


def pages_from_df(pdp, df):
    df = sort_df(pdp, df)

    pages = []
    if df.empty:
        pages.append(pdp._write_page(df, 0))
        return pages

    for i in range(0, len(df), pdp.page_row_capacity):
        page_df = df.iloc[i:i + pdp.page_row_capacity].reset_index(drop=True)
        pages.append(pdp._write_page(page_df, i // pdp.page_row_capacity))

    return pages

# helper function to binary search for correct page index
def find_page_index_binary(pdp, value):
    target = "" if pd.isna(value) else str(value)

    left = 0
    right = len(pdp.chunks) - 1

    while left <= right:
        mid = (left + right) // 2
        page = pdp.chunks[mid]

        first = str(page.get("first", ""))
        last = str(page.get("last", ""))

        if first <= target <= last:
            return mid
        if target < first:
            right = mid - 1
        else:
            left = mid + 1

    if left <= 0:
        return 0
    if left >= len(pdp.chunks):
        return len(pdp.chunks) - 1
    return left


# move out of here if used later in non-initial build
def sort_df(pdp, df):
    if df.empty:
        return df.reset_index(drop=True)
    return df.sort_values(by=pdp.columns[0], kind="stable").reset_index(drop=True)
