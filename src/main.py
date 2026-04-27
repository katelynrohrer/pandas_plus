
import pdp
import time
import utils
import phony_rows


DATA_FILES = [
    "data/small_song.csv",
    "data/small_crime.csv",
    "data/medium_crime.csv",
    "data/medium_song.csv",
    "data/large_crime.csv"
    # "data/large_song.csv", # takes too long to test
]


def main():

    df = pdp.PDplus(DATA_FILES[4]) # by default, sorts by first col

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