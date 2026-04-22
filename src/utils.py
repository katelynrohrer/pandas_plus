
import os
import psutil
import resource
import pandas as pd


def to_mb(bytes_val):
    return round(bytes_val / (1024 * 1024))


DATA_FILES = [
    "data/small_song.csv",
    "data/small_crime.csv",
    "data/medium_la_crime.csv", # unreliably small enough
    "data/medium_song.csv",
    "data/large_song.csv",
    # "data/large_la_crime.csv" # TODO too big for casual testing
]

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
new_row2 = {
    'artist.familiarity': 'AAA',
    'artist.hotttnesss': 'BBB',
    'artist.id': 'CCC',
    'artist.latitude': 'DDD',
    'artist.location': 'EEE',
    'artist.longitude': 'FFF',
    'artist.name': 'GGG',
    'artist.similar': 'HHH',
    'artist.terms': 'III',
    'artist.terms_freq': 'JJJ',
    'release.id': 'KKK',
    'release.name': 'LLL',
    'song.artist_mbtags': 'MMM',
    'song.artist_mbtags_count': 'NNN',
    'song.bars_confidence': 'OOO',
    'song.bars_start': 'PPP',
    'song.beats_confidence': 'QQQ',
    'song.beats_start': 'RRR',
    'song.duration': 'SSS',
    'song.end_of_fade_in': 'TTT',
    'song.hotttnesss': 'UUU',
    'song.id': '00000000',
    'song.key': 'WWW',
    'song.key_confidence': 'XXX',
    'song.loudness': 'YYY',
    'song.mode': 'ZZZ',
    'song.mode_confidence': 'AAAA',
    'song.start_of_fade_out': 'BBBB',
    'song.tatums_confidence': 'CCCC',
    'song.tatums_start': 'DDDD',
    'song.tempo': 'EEEE',
    'song.time_signature': 'FFFF',
    'song.time_signature_confidence': 'GGGG',
    'song.title': 'HHHH',
    'song.year': 'IIII'
}


def iter_files(file_main, files=None):
    if files is None:
        files = DATA_FILES

    for file in files:
        file_main(file)
        yield file


def make_stepper(file_main, files=None):
    gen = None

    def start():
        nonlocal gen
        gen = iter_files(file_main, files)

    def step():
        nonlocal gen
        if gen is None:
            raise StopIteration
        return next(gen)

    return start, step


def run_all(file_main, files=None):
    if files is None:
        files = DATA_FILES

    for file in files:
        print(file)
        file_main(file)
        print()


def get_size_limit():
    mem, _ = resource.getrlimit(resource.RLIMIT_AS)  # virtual limits (e.g. the 1gb limit)

    if mem == -1:
        # in real use case, this would always be used by default
        mem = psutil.virtual_memory().available # physical limits (e.g container capacity)

    size_limit = mem / 15  # pandas typically needs 2-5x the space of the file. we're being conservative here

    return size_limit

def estimate_row_size(file, sample=1000):
    sample_df = pd.read_csv(file, dtype=str, nrows=sample)
    bytes_per_row = sample_df.memory_usage(deep=True).sum() / len(sample_df)
    return bytes_per_row



