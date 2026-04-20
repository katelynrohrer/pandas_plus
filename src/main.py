
import pdp
import utils


def main():
    file = "data/medium_song.csv"
    df = pdp.PDplus(file)
    df.print()



if __name__ == "__main__":
    main()