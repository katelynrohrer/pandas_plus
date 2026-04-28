
import pdp
import time


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
    source_build = "sorted_1gb"
    func = "_lookup"
    file = DATA_FILES[0]
    sort = True

    start = time.time()

    df = pdp.PDplus(file, sort=True, build_name=source_build)

    if not df.cache_is_valid():
        df.abort_cache()
        df.build_cache()
    else:
        df.read_cache()

    df.lookup("0.5")

    end = time.time()

    output = f"File: {file}\n" + \
             f"Sort: {sort}\n" + \
             f"Build: False\n" + \
             f"Time: {(end - start) * 1000:.2f} ms\n\n"

    print(output)

    with open(f"./metrics/{source_build + func}.txt", "a") as f:
        f.write(output)

    # df2 = pdp.PDplus(file, sort=True, build_name=source_build + func)
    # df2.read_cache()
    # df2.print()

# middle = 3AJmjefnCKNq2Vtib5qQSE
# end = 8zzwQwN3jNiK46B2M9kL2Q


if __name__ == "__main__":
    main()