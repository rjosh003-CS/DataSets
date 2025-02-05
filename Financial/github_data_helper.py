import pandas as pd
from pathlib import Path
import os
import time
from typing import Union, Dict
import requests
from tqdm import tqdm


def download_file(url, filename):
    """Downloads a file with a visible progress bar."""
    response = requests.get(url, stream=True)
    
    if response.status_code != 200:
        print(f"Failed to download {url}, HTTP Status: {response.status_code}")
        return
    
    total_size = int(response.headers.get("content-length", 0))
    block_size = 1024  # 1 KB

    with open(filename, "wb") as file, tqdm(
        desc=f"Downloading {filename}",
        total=total_size,
        unit="B",
        unit_scale=True,
        unit_divisor=1024
    ) as progress_bar:
        for chunk in response.iter_content(chunk_size=block_size):
            file.write(chunk)
            progress_bar.update(len(chunk))


def load_raw_data(file_path: Dict = None, out_dir: str = "") -> dict:
    """Downloads raw data files if they don't exist locally and renames keys.

    Args:
        file_path (dict, optional): Dictionary mapping file names to URLs.
        out_dir (str, optional): Directory to save files.

    Raises:
        ValueError: If file_path is None, not a dictionary, or empty.

    Returns:
        dict: Updated file_path dictionary with renamed keys.
    """
    if file_path is None or not isinstance(file_path, dict) or len(file_path) == 0:
        raise ValueError("file_path must be a non-empty dictionary")

    out_dir = os.path.abspath(out_dir)
    os.makedirs(out_dir, exist_ok=True)

    updated_file_path = {}
    for file_name, file_url in file_path.items():
        new_file_name = os.path.join(out_dir, file_name)
        updated_file_path[new_file_name] = file_url

        if not os.path.exists(new_file_name):
            download_file(url=file_url, filename=new_file_name)
        else:
            print(f"{new_file_name} already exists")

    return updated_file_path


def load_git_data(file_path: Dict = None,
                  interval: Union[str, None] = None,
                  out_dir: str = "",
                  debug: bool = False) -> pd.DataFrame:
    """Loads data from a Git repository for reproducibility.

    Args:
        file_path (dict, optional): Dictionary mapping filenames to URLs.
        interval (str, optional): Data interval ('1d', etc.).
        out_dir (str, optional): Output directory.
        debug (bool, optional): Enables debug logs.

    Raises:
        ValueError: If file_path is None, not a dictionary, or empty.

    Returns:
        pd.DataFrame: Loaded data.
    """
    if file_path is None or not isinstance(file_path, dict) or len(file_path) == 0:
        raise ValueError("file_path must be a non-empty dictionary")

    out_dir = os.path.abspath(out_dir)

    # Download files and update file paths
    updated_file_path = load_raw_data(file_path, out_dir)

    if debug:
        print("\nDEBUG INFORMATION")
        print(f"out_dir = {out_dir}")
        print(f"Absolute path = {os.path.abspath(out_dir)}")
        print(f"Directory exists? {os.path.exists(out_dir)}")
        print(f"Contents of {out_dir}: {os.listdir(out_dir) if os.path.exists(out_dir) else 'Directory missing'}")

    time.sleep(1)  # Sleep to prevent Colab caching issues

    ls_files(path=out_dir)

    # Read and concatenate data
    data = pd.DataFrame()
    for file_name in updated_file_path.keys():
        try:
            df = pd.read_csv(file_name, header=[0, 1], index_col=0)
        except pd.errors.EmptyDataError:
            print(f"Skipping empty file: {file_name}")
            continue

        print(f"shape of df from {Path(file_name).name}: {df.shape}")

        df.index = pd.to_datetime(df.index, utc=True).tz_localize(None)
        if interval == "1d":
            df.index = df.index.to_period('D')

        df.index.name = "Date"
        df.columns.names = ['Price', 'Ticker']
        data = pd.concat([data, df])

    print("\nData shape:", data.shape)
    return data


def ls_files(path=".", debug=False):
    """Lists files in the specified directory with their sizes."""
    path = os.path.abspath(path)
    print(f"\nChecking files in directory: {path}", flush=True)

    if not os.path.exists(path):
        print("Directory does not exist.", flush=True)
        return

    files = os.listdir(path)

    if debug:
        print(f"Files found: {files}", flush=True)

    if not files:
        print("No files found in directory.", flush=True)
        return

    for filename in files:
        filepath = os.path.join(path, filename)
        if os.path.isfile(filepath):
            size = os.path.getsize(filepath)
            if size >= 1048576:
                print(f"{filename} {size / 1048576:.2f}M")
            elif size >= 1024:
                print(f"{filename} {size / 1024:.2f}K")
            else:
                print(f"{filename} {size}B", flush=True)
        else:
            print(f"{filename} (Not a file, might be a directory)")




# #  Example of loading files
# # file_path = {
# #     # file 1
# #     "S&P500_5year_daily_data_0.csv": "https://raw.githubusercontent.com/rjosh003-CS/DataSets/refs/heads/main/Data/Financial/OHLC/S%26P500_5year_daily_data_0.csv",

# #     # file 2
# #     "S&P500_5year_daily_data_1.csv": "https://raw.githubusercontent.com/rjosh003-CS/DataSets/refs/heads/main/Data/Financial/OHLC/S%26P500_5year_daily_data_1.csv",

# #     # file 3
# #     "S&P500_5year_daily_data_2.csv": "https://raw.githubusercontent.com/rjosh003-CS/DataSets/refs/heads/main/Data/Financial/OHLC/S%26P500_5year_daily_data_2.csv"
# # }

# # # print(data.shape)
# # data = load_git_data(file_path=file_path, interval='1d', out_dir="data")
# # data.head()
