
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
    file = DATA_FILES[4]
    sort = False
    build = False
    crashed = False

    start = time.time()
    df = pdp.PDplus(file, sort=sort, build_name="unsorted_8gb") # by default, sorts by first col

    try:
        if not df.cache_is_valid():
            df.abort_cache()
            df.build_cache()
            build = True
        else:
            df.read_cache()
    except:
        crashed = True
    finally:
        end = time.time()

        output = f"File: {file}\n" + \
                 f"Sort: {sort}\n" + \
                 f"Build: {build}\n" + \
                 f"Time: {end - start:.2f} seconds\n"
                 # f"Chunks Completed: \n"

        if crashed:
            output += f"Completed = False\nChunks Completed: \n\n"
        else:
            output += "\n"

        print(output)

        with open("./metrics/unsorted_8gb.txt", "a") as file:
            file.write(output)


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