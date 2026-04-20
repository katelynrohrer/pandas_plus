
import pdp
import utils


def main():
    file = "data/medium_song.csv"
    df = pdp.PDplus(file)
    df.print()
    print(df.page_row_capacity)
    new_row = {
        'id': "0VENt14WVFyKtCmhHNLE7W",
        'name': "AAA",
        'album_name': "BBB",
        'artists': "CCC",
        'danceability': "DDD",
        'energy': "EEE",
        'key': "FFF",
        'loudness': "GGG",
        'mode': "HHH",
        'speechiness': "III",
        'acousticness': "JJJ",
        'instrumentalness': "KKK",
        'liveness': "LLL",
        'valence': "MMM",
        'tempo': "NNN",
        'duration_ms': "OOO",
        'lyrics': "PPP",
        'year': "QQQ",
        'genre': "RRR",
        'popularity': "SSS",
        'total_artist_followers': "TTT",
        'avg_artist_popularity': "UUU",
        'artist_ids': "VVV",
        'niche_genres': "WWW"
    }
    print("\n\n\nINSERTING HERE\n\n\n")
    df.insert(new_row)
    df.print()

if __name__ == "__main__":
    main()