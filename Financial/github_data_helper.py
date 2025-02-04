import pandas as pd
import os
import subprocess
import time
from typing import Union, Dict
from pathlib import Path

def load_raw_data(file_path: Dict = None, out_dir: str = "") -> dict:
    """Downloads raw data files if they don't exist locally and renames keys.

    Args:
        file_path (dict, optional): Dictionary mapping file names to URLs. Defaults to None.
        out_dir (str, optional): Directory to save files. Defaults to "".

    Raises:
        ValueError: If file_path is None, not a dictionary, or empty.

    Returns:
        dict: Updated file_path dictionary with renamed keys.
    """
    # Input validation
    if file_path is None:
        raise ValueError("file_path cannot be None")
    if not isinstance(file_path, dict):
        raise ValueError("file_path must be a dictionary")
    if len(file_path) == 0:
        raise ValueError("file_path cannot be empty")

    # Ensure output directory is absolute path
    out_dir = os.path.abspath(out_dir)
    if len(out_dir) > 0:
        os.makedirs(out_dir, exist_ok=True)

    # Rename keys and download files
    updated_file_path = {}  
    for file_name, file_url in file_path.items():
        new_file_name = os.path.join(out_dir, file_name)  
        updated_file_path[new_file_name] = file_url  

        if not os.path.exists(new_file_name):
            command = f'wget "{file_url}" -O "{new_file_name}" --show-progress'
            print(f"Downloading {file_name}...", flush=True)
            subprocess.run(command, shell=True, check=True) 
        else:
            print(f"{new_file_name} already exists")

    return updated_file_path


def load_git_data(file_path: Dict = None,
                  interval: Union[str, None] = None,
                  out_dir: str = "",
                  debug: bool =False) -> pd.DataFrame:
    """
    Loads data from a Git repository for reproducibility.

    Args:
        file_path (dict, optional): Dictionary mapping filenames to URLs. Defaults to None.
        interval (str, optional): Data interval ('1d', etc.). Defaults to None.
        out_dir (str, optional): Output directory. Defaults to "".

    Raises:
        ValueError: If file_path is None, not a dictionary, or empty.

    Returns:
        pd.DataFrame: Loaded data.
    """
    # Input validation
    if file_path is None:
        raise ValueError("file_path cannot be None")
    if not isinstance(file_path, dict):
        raise ValueError("file_path must be a dictionary")
    if len(file_path) == 0:
        raise ValueError("file_path cannot be empty")

    # Convert to absolute path
    out_dir = os.path.abspath(out_dir)

    # Download raw files and get updated file_path
    updated_file_path = load_raw_data(file_path, out_dir)  

    if debug:
        # Debugging output
        print("\n\n", "**" * 15)
        print("\n$ls -lh")
        print(f"DEBUG: out_dir = {out_dir}")
        print(f"DEBUG: Absolute path = {os.path.abspath(out_dir)}")
        print(f"DEBUG: Directory exists? {os.path.exists(out_dir)}")
        print(f"DEBUG: Contents of {out_dir}: {os.listdir(out_dir) if os.path.exists(out_dir) else 'Directory missing'}")

    # Sleep to prevent Colab caching issues
    time.sleep(1)

    # List files
    ls_files(path=out_dir)
    print("\n\n", "**" * 15)

    # Read and concatenate data
    data = pd.DataFrame()
    for file_name in updated_file_path.keys():
        try:
            df = pd.read_csv(f"{file_name}", header=[0, 1], index_col=0)
        except pd.errors.EmptyDataError:
            print(f"Skipping empty file: {file_name}")
            continue
        file = Path(file_name)
        print(f"shape of df from {file.name} :  {df.shape}")

        # Process data
        df.index = pd.to_datetime(df.index, utc=True).tz_localize(None)
        if interval == "1d":
            df.index = df.index.to_period('D')
        df.index.name = "Date"
        df.columns.names = ['Price', 'Ticker']
        data = pd.concat([data, df])

    print("\n\ndata shape : ", data.shape)
    return data


def ls_files(path=".", debug=False):
    """Lists files in the specified directory with their sizes."""
    path = os.path.abspath(path)  # Convert to absolute path
    print(f"\n\nChecking files in directory: {path}", flush=True)

    if not os.path.exists(path):
        print("Directory does not exist.", flush=True)
        return

    files = os.listdir(path)

    if debug:
        print(f"Files found: {files}", flush=True)  # Debug print

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
