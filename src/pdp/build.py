
import pandas as pd


def build_pages(pdp):
    if pdp.can_fit_in_mem:
        df = pd.read_csv(pdp.file, dtype=str)
        return pdp._pages_from_df(df)
    if pdp.sort_by is None:
        return pages_from_unsorted_chunks(pdp)
    return pages_from_buckets(pdp)

def pages_from_unsorted_chunks(pdp):
    pages = []
    chunk_num = 0
    print("starting unsorted paged build...")

    for chunk in pd.read_csv(pdp.file, dtype=str, chunksize=pdp.page_row_capacity):
        chunk_num += 1
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
    return pdp.pages

def pages_from_buckets(pdp):
    pages = []

    for letter in "ABCDEFGHIJKLMNOPQRSTUVWXYZ":
        empty_df = pd.DataFrame(columns=pdp.columns)
        page = pdp._write_page(empty_df, idx=letter)
        pages.append(page)

    pdp.pages = pages
    pdp._write_index(pdp.pages)

    key_col = pdp.columns[0]
    chunk_num = 0
    print("starting paged build...")

    # use build in generator for chunking
    for chunk in pd.read_csv(pdp.file, dtype=str, chunksize=pdp.page_row_capacity):
        chunk_num += 1
        print(f"processing chunk {chunk_num}...")
        updates = {}

        use_starter_buckets = any(page["first"] == "" and page["last"] == "" for page in pdp.pages)

        for _, row in chunk.iterrows():
            row_dict = row.to_dict()
            row_value = row_dict.get(key_col, "")

            if use_starter_buckets:
                page_idx = _starter_bucket_index(row_value)
            else:
                page_idx = pdp._find_page_index_binary(row_value)

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

            while page_idx < len(pdp.pages):
                current_df = pdp._load_page(page_idx)
                if not pdp._page_is_full(current_df):
                    break

                before = len(pdp.pages)
                pdp._split_page(page_idx, current_df)
                after = len(pdp.pages)
                offset += after - before

    pdp._remove_empty_pages()
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

    return pages

# move out of here if used later in non-initial build
def sort_df(pdp, df):
    if df.empty:
        return df.reset_index(drop=True)

    if pdp.sort_by is None:
        return df.reset_index(drop=True) # todo why?

    return df.sort_values(by=pdp.sort_by, kind="stable").reset_index(drop=True)