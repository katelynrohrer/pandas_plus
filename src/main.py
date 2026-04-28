
import pdp
import time
import phony_rows


DATA_FILES = [
    "data/small_song.csv", # about 1 chunk on 1GB memory
    "data/small_crime.csv", # about 1 chunk on 1GB memory
    "data/medium_crime.csv", # about 32 chunks on 1GB memory
    "data/medium_song.csv", # about 31 chunks on 1GB memory

    # these large files take several hours to sorted build
    "data/large_song.csv", # about 566 chunks on 1GB memory
    "data/large_crime.csv", # about 1030 chunks on 1GB memory
]


def main():
    file = DATA_FILES[0]
    sort = False

    start = time.time()

    df = pdp.PDplus(file, sort=True, build_name="sorted_1gb")

    if not df.cache_is_valid():
        df.abort_cache()
        df.build_cache()
    else:
        df.read_cache()

    row = {k: "item" for k in df.columns}
    df.insert(row)
    df.make_snapshot("sorted_insert_1gb")


    end = time.time()

    output = f"File: {file}\n" + \
             f"Sort: {sort}\n" + \
             f"Build: False\n" + \
             f"Time: {end - start:.2f} seconds\n\n"

    print(output)

    with open("./metrics/sorted_insert_1gb.txt", "a") as file:
        file.write(output)




if __name__ == "__main__":
    main()