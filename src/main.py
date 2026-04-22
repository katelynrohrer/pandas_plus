
import pdp
import utils


def main():
    file = "data/medium_song.csv"
    df = pdp.PDplus(file) # by default, sorts by first col

    if not df.cache_is_valid():
        df.abort_cache()
        df.build_cache()
    else:
        df.read_cache()

    df.print()
    print("\n\n\nINSERTING HERE\n\n\n")
    for i in range(100):
        df.insert(utils.new_row)
    df.print()
    print("\n\n\nDELETING HERE\n\n\n")
    df.delete(utils.new_row["id"], single=False)

    df.print()


if __name__ == "__main__":
    main()