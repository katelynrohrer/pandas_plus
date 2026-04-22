
import pdp
import utils


def main():
    # file1 = "data/medium_song.csv"
    # df1 = pdp.PDplus(file1) # by default, sorts by first col
    #
    # if not df1.cache_is_valid():
    #     df1.abort_cache()
    #     df1.build_cache()
    # else:
    #     df1.read_cache()
    #
    # df1.print()
    # print("\n\n\nINSERTING HERE\n\n\n")
    # for i in range(100):
    #     df1.insert(utils.new_row)
    # df1.print()
    # print("\n\n\nDELETING HERE\n\n\n")
    # df1.delete(utils.new_row["id"], single=False)
    #
    # df1.print()



    file2 = "data/small_song.csv"
    df2 = pdp.PDplus(file2, sort_col="song.id") # by default, sorts by first col

    if not df2.cache_is_valid():
        df2.abort_cache()
        df2.build_cache()
    else:
        df2.read_cache()

    df2.print()
    print("\n\n\nINSERTING HERE\n\n\n")
    for i in range(100):
        df2.insert(utils.new_row2)
    df2.print()
    print("\n\n\nDELETING HERE\n\n\n")
    df2.delete(utils.new_row2["song.id"], single=False)

    df2.print()



if __name__ == "__main__":
    main()