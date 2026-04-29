
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
    source_build = "unsorted_1gb"
    func = "_insert"
    key = "8zzwQwN3jNiK46B2M9kL2Q"

    file = DATA_FILES[3]
    sort = False

    start = time.time()

    df = pdp.PDplus(file, sort=sort, build_name=source_build)

    if not df.cache_is_valid():
        df.abort_cache()
        df.build_cache()
    else:
        df.read_cache()

    row = {k: key for k in df.columns}
    df.insert(row)

    end = time.time()

    output = f"File: {file}\n" + \
             f"Sort: {sort}\n" + \
             f"Build: False\n" + \
             f"Time: {(end - start) * 1000:.2f} ms\n\n"

    print(output)


    with open(f"./metrics/pdp_{source_build + func}.txt", "a") as f:
        f.write(output)

# CRIMES
# top = 200705253
# middle = 221413100
# end = 241804338

# SONGS
# top = 000RwtVtOYEHSjDoj6shff, 0VERkxkJX3OwQMBppqcbIA
# middle = 3AJmjefnCKNq2Vtib5qQSE
# end = 8zzwQwN3jNiK46B2M9kL2Q

# File: data / medium_crime_sorted.csv
# Predicate: lambda x: float(x["DR_NO"]) >= 231109345
# Predicate: lambda x: x["DR_NO"].startswith("23")
# Predicate: lambda x: x["DR_NO"].startswith("23") and x["AREA"] == "20"
#
# File: data / medium_song_sorted.csv
# Predicate: lambda x: x["id"].startswith("6X")
# Predicate: lambda x: x["id"].startswith("7z")
# Predicate: lambda x: x["id"].startswith("7z") and float(x["avg_artist_popularity"]) > 50


if __name__ == "__main__":
    main()













