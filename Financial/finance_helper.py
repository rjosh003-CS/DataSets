import pandas as pd
import numpy as np

def calculate_cumulative_inflation_implied_inflation(principal_value: float = None,
                                                     current_value: float = None,
                                                     implied_inflation: float = None,
                                                     n: int = 10,
                                                     currency_symbol: str = "£") -> None:
    """
    Calculate break-even future value using implied inflation.

    Args:
        principal_value: Initial value of the asset (default is None).
        current_value: Present value of the asset (default is None).
        implied_inflation: Average implied inflation rate over the period (default is None).
        n: Number of years (default is 10).
        currency_symbol: Currency symbol to use in the output (default is £).

    Returns:
        None
    """
    if principal_value is None or implied_inflation is None or current_value is None:
        raise ValueError("principal_value, current_value, and implied_inflation cannot be None!")

    print(f"Principal value: {currency_symbol}{principal_value:,}")
    print(f"Current value: {currency_symbol}{current_value:,}")
    print(f"Implied inflation: {implied_inflation:.2%}")

    # Break-even future value
    FV = principal_value * (1 + implied_inflation) ** n
    print(f"Break-Even Future Value: {currency_symbol}{FV:,.2f}")

    # Difference in value
    diff = current_value - FV
    if diff < 0:
        print(f"Loss of: {currency_symbol}{abs(diff):,.2f}")
    else:
        print(f"Profit of: {currency_symbol}{diff:,.2f}")

def calculate_cumulative_inflation(principal_value: float = None,
                                   current_value: float = None,
                                   cumulative_inflation: float = None,
                                   currency_symbol: str = "£") -> None:
    """
    Calculate break-even future value using cumulative inflation.

    Args:
        principal_value: Initial value of the asset (default is None).
        current_value: Present value of the asset (default is None).
        cumulative_inflation: Cumulative inflation rate (default is None).
        currency_symbol: Currency symbol to use in the output (default is £).

    Returns:
        None
    """
    if principal_value is None or cumulative_inflation is None or current_value is None:
        raise ValueError("principal_value, current_value, and cumulative_inflation cannot be None!")

    print(f"Principal value: {currency_symbol}{principal_value:,}")
    print(f"Current value: {currency_symbol}{current_value:,}")
    print(f"Cumulative inflation: {cumulative_inflation:.2%}")

    # Break-even future value
    FV = principal_value / cumulative_inflation
    print(f"Break-Even Future Value: {currency_symbol}{FV:,.2f}")

    # Difference in value
    diff = current_value - FV
    if diff < 0:
        print(f"Loss of: {currency_symbol}{abs(diff):,.2f}")
    else:
        print(f"Profit of: {currency_symbol}{diff:,.2f}")
