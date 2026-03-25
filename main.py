
import pandas as pd
from icecream import ic


def main():
    df = pd.read_csv("data/medium_song_songs.csv")
    ic(df)

if __name__ == "__main__":
    main()