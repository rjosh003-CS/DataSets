import pandas as pd
from datetime import datetime

def load_data_via_yfinance_api(df:pd.DataFrame = None,
                               symbols: List[str] = None, 
                               interval = '1d')->pd.DataFrame:
    """
    Loads data from Yahoo Finance API into a DataFrame.
    Used to download OHLC data for a list of symbols of similar timeframe(start and end) data and interval for comaprison.

    (Helpful for downloading list of stock of another index on same timeframe.)

    "Common Usage Loading stocks for different index funds of similar timeframe for analysis purpose"

    Args:
        df (pd.DataFrame): df of a stock or list of stock
        symbols (List[str]): list of symbols of stocks
        interval (str, optional): string value of interval. Defaults to '1d'.

    Returns:
        pd.DataFrame: DataFrame of OHLC data for the given symbols.
    """
    #checks for params
    if df is None:
        raise ValueError("df cannot be None")

    if symbols is None:
        raise ValueError("symbols cannot be None")

    if not isinstance(symbols, list):
        raise ValueError("symbols must be a list")

    if len(symbols) == 0:
        raise ValueError("symbols cannot be empty")

    # checking if the index is atleast pd.Period or pd.datetime type
    index_dtype = df.index.dtype
    if not (pd.api.types.is_datetime64_any_dtype(index_dtype) or 
            pd.api.types.is_period_dtype(index_dtype)):
        raise TypeError("DataFrame index must be either Period or DatetimeIndex.")

    # getting the start and end value
    start = df.index[0]
    end = df.index[-1]

    # printing out the interval param
    print(f"interval: {interval}")

    # convert pd.Period to pd.TimeStamp()
    if isinstance(start, pd.Period):
        print(f"Converting pd.Period to pd.Timestamp! ...")
        start = start.to_timestamp()
        end = end.to_timestamp()

    # checking if the start timestamp older than end timestamp
    if start > end:
        start, end = end, start
    
    print(f"start: {start} \nend: {end}")

    # Ensure start and end are datetime objects
    start = pd.to_datetime(start).to_pydatetime()
    end = pd.to_datetime(end).to_pydatetime()

    # loading data via yf.finance
    import yfinance as yf
    # add end offset by 1 day as yfinance will exclude the end date
    end = end + pd.DateOffset(days=1)
    stocks = yf.download(symbols, start=start, end=end,
                         interval=interval, auto_adjust=False)

    return stocks


# Example
# stocks = load_data_via_yfinance_api(df_by_sectors['Technology'], symbols)
