# TODO test that this works correctly (I downloaded it manually + am on hotspot right now)
import kagglehub
import os

DATA_DIR = "./data"
os.makedirs(DATA_DIR, exist_ok=True)

# small files
path = kagglehub.dataset_download("mexwell/10k-song-dataset", path=DATA_DIR)
print("Path to dataset files:", path)

path = kagglehub.dataset_download("sohier/crime-in-baltimore")
print("Path to dataset files:", path)


# medium files

path = kagglehub.dataset_download("abdullahmazari/crime-data-of-los-angeles-from-2020-to-2025", path=DATA_DIR)
print("Path to dataset files:", path)

path = kagglehub.dataset_download("serkantysz/550k-spotify-songs-audio-lyrics-and-genres", path=DATA_DIR)
print("Path to dataset files:", path)


# large files
path = kagglehub.dataset_download("aliafzal9323/los-angeles-crime-data-2020-2026", path=DATA_DIR)
print("Path to dataset files:", path)

path = kagglehub.dataset_download("carlosgdcj/genius-song-lyrics-with-language-information", path=DATA_DIR)
print("Path to dataset files:", path)

