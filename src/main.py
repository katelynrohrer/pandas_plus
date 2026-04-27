
import pdp
import time
import phony_rows


DATA_FILES = [
    "data/small_song.csv", # 1 chunk on 1GB memory
    "data/small_crime.csv", # 1 chunk on 1GB memory
    "data/medium_crime.csv",
    "data/medium_song.csv",

    # these large files take several hours to sorted build
    "data/large_song.csv", # 565 chunks on 1GB memory
    "data/large_crime.csv", # 1029 chunks on 1GB memory
]


def main():

    df = pdp.PDplus(DATA_FILES[5]) # by default, sorts by first col

    if not df.cache_is_valid():
        df.abort_cache()
        df.build_cache()
    else:
        df.read_cache()

    # # phony data. only need to know 'id': "0VENt14WVFyKtCmhHNLE7W",
    # df.insert(phony_rows.medium_song_row)
    # df.make_snapshot("inserted")
    # df.print()
    # df.delete("0VENt14WVFyKtCmhHNLE7W", single=True)
    #
    # df2 = df.filter(lambda x: x['danceability'] > 0.5, save_as="danceable")
    #
    # # df3 is now saved in a temp build
    # df3 = df2.project(["id", "danceability", "name", "artists"])
    # print(df3.count(lambda x: True)) # counts rows (everything is True)
    #
    # # default has no changes but this does close all other builds
    # df.close_project("default")




if __name__ == "__main__":
    main()