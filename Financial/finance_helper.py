import pandas as pd
import numpy as np

def calculate_cumulative_inflation_implied_inflation(principal_value: float=None,
                                   current_value: float=None,
                                   implied_inflation: float= None,
                                    n: int=10,
                                    currency_symbol: str = "£"
                                ) -> None:
    """
        Calculate break even future value using cummulative inflation.

        pricipal_value: initial value of the asset
            defalue is None
        current_value: present value of the asset
            defalue is None
        implied_inflation: average implied inflation of the country over the period in time intervale of years
            defalue is None
        n: number of years
            defalue is 10
        currency_symbol: currency symbol to be used in the output
            defalue is £

        returns: None
    """
    # check
    if principal_value is None or cumulative_inflation is None or current_value is None:
        raise(ValueError("principal_value, current_value and cumulative_inflation cannot be none! "))


    PV = principal_value
    i = implied_inflation

    print(f"Principal value: {currency_symbol}{principal_value:,}")
    print(f"Current value: {currency_symbol}{current_value:,}")
    print(f"cummulatice implied inflation: {cumulative_inflation}")

    #  Break even Future value based on current value
    FV = PV * (1 + i)**n

    #  Calculating P/L using Breakeven value with the current value(Future Value)
    print(f"Break-Even Future Value: {currency_symbol}{FV:,.2f}")
    diff = avg_current_price - FV
    if diff < 0:
        print(f"\nloss of : {currency_symbol}{diff:,.2f}")
    else:
        print(f"\nprofit of : {currency_symbol}{diff:,.2f}")




def calculate_cumulative_inflation(principal_value: float=None,
                                   current_value: float=None,
                                   cumulative_inflation: float= None,
                                   currency_symbol: str="£") -> None:

    """
        Calculate break even future value using cummulative inflation.

        Args:

        pricipal_value: initial value of the asset
            defalue is None
        current_value: present value of the asset
            defalue is None
        cumulative_inflation: cumulative inflation of the country over the
        period in time intervale of years
            defalue is None
        currency_symbol: currency symbol to be used in the output
            defalue is £
            
        returns: None
    """

    # check
    if principal_value is None or cumulative_inflation is None or current_value is None:
        raise(ValueError("principal_value, current_value and cumulative_inflation cannot be none! "))

    PV = principal_value
    print(f"Principal value: {currency_symbol}{principal_value:,}")
    print(f"Current value: {currency_symbol}{current_value:,}")
    print(f"cummulatice implied inflation: {cumulative_inflation}")

    # Break even future value
    FV = PV / (cumulative_inflation)

    #  Calculating the difference in current value from break even future value
    diff = avg_current_price - FV

    #  Calculating P/L using Breakeven value with the current value(Future Value)
    print(f"\nBreak-Even Future Value: {currency_symbol}{FV:,.2f}")
    print(f"\ndifference in Value: {currency_symbol}{diff:,.2f}")

    if diff < 0:
        print(f"\nloss of : {currency_symbol}{diff:,.2f}")
    else:
        print(f"\nprofit of : {currency_symbol}{diff:,.2f}")
