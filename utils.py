import logging
from typing import Tuple

import pandas as pd


def load_data(filepath: str) -> pd.DataFrame:
    """
    Loads data from parquet into a dataframe
    :param filepath: String, file path to a parquet data format
    :return: DataFrame, loaded from parquet file
    """
    try:
        expected_data = pd.read_parquet(filepath)
    except Exception as e:
        raise e

    return expected_data


def analyze_data(
    actual_df: pd.DataFrame, expected_df: pd.DataFrame
) -> Tuple[str, pd.DataFrame, int]:
    """
    Compare expected df result to actual df result, with summary of vital information
    :param actual_df: DataFrame, actual dataframe
    :param expected_df: String, expected dataframe
    :return: Tuple of [string, dataframe, integer] for most expensive product, missing products and count of product
    price matches from actual to expected data
    """
    # Q1: What is the most expensive product according to actual data?
    most_expensive_product: str = actual_df.loc[actual_df["final_price"].idxmax()][
        "title"
    ]

    # Q2: What product is missing in expected data?
    missing_products: pd.DataFrame = pd.merge(
        actual_df,
        expected_df,
        how="left",
        on="id",
        indicator=True,
        suffixes=("_actual", "_expected"),
    )
    missing_products = missing_products[missing_products["_merge"] == "left_only"]

    # Q3: For how many rows final price matches between actual and expected data?
    merged_df = pd.merge(
        actual_df[["id", "final_price"]],
        expected_df[["id", "final_price"]],
        on="id",
        suffixes=("_actual", "_expected"),
    )
    price_matches: int = int(
        (merged_df["final_price_actual"] == merged_df["final_price_expected"]).sum()
    )

    logging.info(f"Completed Analysis of actual and expected product data")

    return most_expensive_product, missing_products, price_matches
