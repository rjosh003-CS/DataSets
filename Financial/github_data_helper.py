import pandas as pd
import os
from typing import Union, Dict

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

    # Create output directory if needed
    if len(out_dir) > 0:
        os.makedirs(out_dir, exist_ok=True)

    # Rename keys and download files
    updated_file_path = {}  # Store updated file paths
    for file_name, file_url in file_path.items():
        # Rename the key (file_name) by adding out_dir as a prefix
        new_file_name = os.path.join(out_dir, file_name)  
        updated_file_path[new_file_name] = file_url  # Add to the updated dictionary

        if not os.path.exists(new_file_name):
            # Using wget to download to the new file name
            command = f'wget "{file_url}" -O "{new_file_name}" -q --show-progress'
            os.system(command)  
        else:
            print(f"{new_file_name} already exists")

    return updated_file_path  # Return the updated dictionary


def load_git_data(file_path: Dict = None,
                  interval: Union[str, None] = None,
                  out_dir: str = "") -> pd.DataFrame:
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

    # Download raw files and get updated file_path
    updated_file_path = load_raw_data(file_path, out_dir)  # Get updated dict

    # List files with sizes (using updated file paths)
    print("\n\n", "**" * 15)
    print("\n$ls -lh")
    ls_files(out_dir=out_dir)
    print("\n\n", "**" * 15)

    # Read and concatenate data (using updated file paths)
    data = pd.DataFrame()
    for file_name in updated_file_path.keys():  # Iterate over updated keys
        try:
            df = pd.read_csv(f"{file_name}", header=[0, 1], index_col=0)
        except pd.errors.EmptyDataError:
            print(f"Skipping empty file: {file_name}")
            continue
        print(df.shape)

        # Process data
        df.index = pd.to_datetime(df.index, utc=True).tz_localize(None)
        if interval == "1d":
            df.index = df.index.to_period('D')
        df.index.name = "Date"
        df.columns.names = ['Price', 'Ticker']
        data = pd.concat([data, df])

    print("\n\ndata shape : ", data.shape)
    return data


def ls_files(path="."):
   """Lists files in the specified directory with their sizes."""
   for filename in os.listdir(path):
       filepath = os.path.join(path, filename)
       size = os.path.getsize(filepath)
       if size >= 1048576:
           print(f"{filename} {size / 1048576:.2f}M")
       elif size >= 1024:
           print(f"{filename} {size / 1024:.2f}K")
       else:
           print(f"{filename} {size}B")



#  Example of loading files
# file_path = {
#     # file 1
#     "S&P500_5year_daily_data_0.csv": "https://raw.githubusercontent.com/rjosh003-CS/DataSets/refs/heads/main/Data/Financial/OHLC/S%26P500_5year_daily_data_0.csv",

#     # file 2
#     "S&P500_5year_daily_data_1.csv": "https://raw.githubusercontent.com/rjosh003-CS/DataSets/refs/heads/main/Data/Financial/OHLC/S%26P500_5year_daily_data_1.csv",

#     # file 3
#     "S&P500_5year_daily_data_2.csv": "https://raw.githubusercontent.com/rjosh003-CS/DataSets/refs/heads/main/Data/Financial/OHLC/S%26P500_5year_daily_data_2.csv"
# }

# # print(data.shape)
# data = load_git_data(file_path=file_path, interval='1d', out_dir="data")
# data.head()
