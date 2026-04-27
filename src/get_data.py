
import os
import kagglehub

DATA_DIR = "./data"
KAGGLE_CACHE_DIR = os.path.join(DATA_DIR, "kaggle_cache")

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(KAGGLE_CACHE_DIR, exist_ok=True)
os.environ["KAGGLEHUB_CACHE"] = KAGGLE_CACHE_DIR




def download_datasets(datasets):
    for dataset in datasets:
        path = kagglehub.dataset_download(dataset)
        print("Path to dataset files:", path)


# small files
download_datasets([
    # small files
    "mexwell/10k-song-dataset",
    "sohier/crime-in-baltimore",

    # medium files
    "abdullahmazari/crime-data-of-los-angeles-from-2020-to-2025",
    "serkantysz/550k-spotify-songs-audio-lyrics-and-genres",

    # large files
    "aliafzal9323/los-angeles-crime-data-2020-2026",
])


# attempted this one but data content was too large to test with
# (took several hours to build each time)

# download_datasets([
#     "carlosgdcj/genius-song-lyrics-with-language-information",
# ])
