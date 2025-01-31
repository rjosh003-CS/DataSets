from collections.abc import Callable
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import yfinance as yf

from typing import Tuple, List, Callable
from typing_extensions import Annotated

import zenml
from zenml import step, pipeline
from zenml.config import DockerSettings
from abc import ABC, abstractmethod


class DataStrategy(ABC):
    """Abstract base class for data handling."""

    def __init__(self,
                 strategy: List[Callable[[pd.DataFrame], pd.DataFrame]] = None
    ):
        self.strategy = strategy or []  # Initialize with an empty list if not provided

    @abstractmethod
    def handle_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """Abstract method to handle data."""
        pass


class DataLoading(DataStrategy):
    """Data loading strategy."""

    @step(name="Data Loading step")
    def handle_data(self, data: pd.DataFrame = None) -> pd.DataFrame:
        """Loads data and applies strategy functions."""
        if data is None:
            # Example: Load data from a CSV file
            data = pd.DataFrame({'A': [1, 2, 3], 'B': [4, 5, 6]})  
        for func in self.strategy:
            data = func(data)
        return data


class DataTransformation(DataStrategy):
    """Data transformation strategy."""

    @step(name="Data Transformation step")
    def handle_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """Applies strategy functions to the data."""
        for func in self.strategy:
            data = func(data)
        return data


class DataCleaning(DataStrategy):
    """Data cleaning strategy."""

    @step(name="Data Cleaning step")
    def handle_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """Applies strategy functions for data cleaning."""
        for func in self.strategy:
            data = func(data)
        return data


@pipeline(enable_cache=True)
def data_processing_pipeline(
    data_loader: DataLoading,
    data_transformer: DataTransformation,
    data_cleaner: DataCleaning,
):
    """Data processing pipeline."""
    data = data_loader.handle_data() 
    transformed_data = data_transformer.handle_data(data=data)
    cleaned_data = data_cleaner.handle_data(data=transformed_data)
    return cleaned_data



"""
# Another way - to create step function, provided the handle_data() function is not a step function, inside the class definiton.

@step
def data_loader_step(data_strategy: DataStrategy) -> pd.DataFrame:
    return data_strategy.handle_data()

@step
def data_transformer_step(data: pd.DataFrame, data_strategy: DataStrategy) -> pd.DataFrame:
    return data_strategy.handle_data(data)

@step
def data_cleaner_step(data: pd.DataFrame, data_strategy: DataStrategy) -> pd.DataFrame:
    return data_strategy.handle_data(data)
"""


"""
Example: Based on the @step decorator used over the handle_data function inside the Data Class 

@pipeline(enable_cache=True)
def data_processing_pipeline(
    data_loader: DataStrategy,
    data_transformer: DataStrategy,
    data_cleaner: DataStrategy
):
    data = data_loader_step(data_strategy=data_loader)
    transformed_data = data_transformer_step(data=data,
                                             data_strategy=data_transformer)
    cleaned_data = data_cleaner_step(data=transformed_data,
                                     data_strategy=data_cleaner)
    return cleaned_data


if __name__ == "__main__":
    # docker_settings = DockerSettings(required_integrations=["docker"])

    # Data loading
    data_loader = DataLoading()
    # Data Transformation
    data_transformer = DataTransformation()
    # Data Cleaning
    data_cleaner = DataCleaning()

    # Data preprocessing pipeline
    data_processing_pipeline(data_loader=data_loader,
                            data_transformer=data_transformer, data_cleaner=data_cleaner)

"""


"""
Example: How to create list of strategy and passed them to the sub-class of the DataStrategy class for data pre-processing

if __name__ == "__main__":
    # Define strategies for each stage
    loading_strategy = [
        lambda df: df.rename(columns={'A': 'ColumnA', 'B': 'ColumnB'}),
    ]
    transformation_strategy = [
        lambda df: df.assign(C=df['ColumnA'] + df['ColumnB']),
    ]
    cleaning_strategy = [
        lambda df: df.dropna(),
    ]

    # Instantiate ETL components with strategies
    data_loader = DataLoading(strategy=loading_strategy)
    data_transformer = DataTransformation(strategy=transformation_strategy)
    data_cleaner = DataCleaning(strategy=cleaning_strategy)

    # Run the pipeline
    data_processing_pipeline(
        data_loader=data_loader,
        data_transformer=data_transformer,
        data_cleaner=data_cleaner,
    )

"""
