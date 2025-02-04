import pandas as pd
import pandas.testing as tm  # Import the testing module

def compare_dataframes(df1:pd.DataFrame = None,
                       df2: pd.DataFrame = None) -> None:
    """
    Compares two DataFrames using pandas.testing and prints a detailed analysis.

    Args:
        df1 (pd.DataFrame): The first DataFrame.
        df2 (pd.DataFrame): The second DataFrame.

    Returns:
        None
    """

    try:
        # Attempt to assert that DataFrames are equal
        tm.assert_frame_equal(df1, df2)
        print("DataFrames are identical.")

    except AssertionError as e:
        print("DataFrames are different:")
        print(e)  # Print the AssertionError message

        # Further analysis:
        # 1. Check for differences in shape
        if df1.shape != df2.shape:
            print(f"Shape difference: df1 - {df1.shape}, df2 - {df2.shape}")

        # 2. Check for differences in index
        if not df1.index.equals(df2.index):
            print("Index difference:")
            print("df1 index:", df1.index)
            print("df2 index:", df2.index)

        # 3. Check for differences in columns
        if not df1.columns.equals(df2.columns):
            print("Columns difference:")
            print("df1 columns:", df1.columns)
            print("df2 columns:", df2.columns)
            
        # 4. Check for differences in column types
        df1_dtypes = df1.dtypes
        df2_dtypes = df2.dtypes
        
        if not df1_dtypes.equals(df2_dtypes):
            diff_cols = df1_dtypes.index[df1_dtypes != df2_dtypes]
            print(f"Column type differences: on columns : {diff_cols.to_list()}")
        
        # 5. Check for exact element-wise differences
        # (Optional, but can be expensive for large DataFrames)
        try:
            diff = df1.compare(df2)
            print("Detailed element-wise differences:")
            print(diff)
        except ValueError:
            # Handle ValueError for large differences
            print("Too many element-wise differences to display.")




def compare_dfs(df1: pd.DataFrame = None,
                df2: pd.DataFrame = None,
                to_timestamp=True) -> None:
    """
    Compares two DataFrames using pandas.testing and prints a detailed analysis.

    Args:
        df1 (pd.DataFrame): The first DataFrame.
        df2 (pd.DataFrame): The second DataFrame.

        to_timestamp (bool): If True, converts PeriodIndex to DatetimeIndex.

    Returns:
        None
    """

    # checks
    if df1 is None:
        raise ValueError("df1 cannot be None")

    if df2 is None:
        raise ValueError("df2 cannot be None")
    
    print(f"to_timestamp: {to_timestamp}")

    # keeping sure no data manipulation happens on the real data
    df1_copy = df1.copy()
    df2_copy = df2.copy()

    if to_timestamp:
        if isinstance(df1_copy.index, pd.PeriodIndex):
            # cast PeriodIndex to DatetimeIndex
            df1_copy.index = df1_copy.index.to_timestamp()

        if isinstance(df2_copy.index, pd.PeriodIndex):
            # cast PeriodIndex to DatetimeIndex
            df2_copy.index = df2_copy.index.to_timestamp()
    
    # compare dataframe
    compare_dataframes(df1_copy, df2_copy)


# Example
# print("\ncomparing data and stocks: \n")
# cmp_df(data, stocks)
# print("*"*30)

# # finding if any of the columns has its complete Price column as nul/na
# # .all() to find the list of ticker that have Price column as na
# stocks.loc[:, stocks.dropna(axis=0).isna().any(axis=0)].all()

# # finding the list of columns in a Multi-Indexed df at level 0
# stocks.columns.get_level_values(0).value_counts()


def cmp_dfs(df1: pd.DataFrame,
            df2: pd.DataFrame,
            to_timestamp=True) -> None:
    
    """
    Compares two DataFrames using pandas.testing and prints a detailed analysis.

    Args:
        df1 (pd.DataFrame): The first DataFrame.
        df2 (pd.DataFrame): The second DataFrame.
    
    Returns:
        None
    """
    # checks
    if df1 is None:
        raise ValueError("df1 cannot be None")

    if df2 is None:
        raise ValueError("df2 cannot be None")
    
    print(f"to_timestamp: {to_timestamp}")

    if to_timestamp:
        if isinstance(df1.index, pd.PeriodIndex):
            # cast PeriodIndex to DatetimeIndex
            df1.index = df1.index.to_timestamp()

        if isinstance(df2.index, pd.PeriodIndex):
            # cast PeriodIndex to DatetimeIndex
            df2.index = df2.index.to_timestamp()
    
    # compare dataframe
    try:
        tm.assert_frame_equal(df1, df2)
        print("DataFrames are identical.")
    except AssertionError as e:
        print("DataFrames are different:")
        print(e)  # Print the AssertionError message

