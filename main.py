import json
import logging
import os
from typing import Dict, Tuple

import pandas as pd
import requests

logging.basicConfig(format="%(asctime)s -> %(message)s", level=logging.DEBUG)


def fetch_actual_data(url: str, limit: int, skip: int) -> str:
    """
    Fetches product data from API/url
    :param url: String, Url to be scraped
    :param limit: Integer, limit size for pagination
    :param skip: Integer, skip size to determine where to start next pagination
    :return: String, filepath actual data is written out to
    """
    path, f_name = "./data/raw/", "raw_product_data.parquet"
    if not os.path.exists(path):
        os.mkdir(path)

    params = {"limit": limit, "skip": skip}  # Pagination parameters
    products = []
    total_products: int = 0
    pre_flight: requests.Response = requests.get(url, params={"limit": 1})
    if pre_flight.status_code == 200:
        total_products = pre_flight.json()["total"]
    else:
        raise requests.exceptions.RequestException

    while True:
        data = requests.get(url, params=params).json()
        products.extend(data["products"])
        params["skip"] += data["limit"]

        if len(products) >= total_products:
            break

    # Calculate final price for each product
    for product in products:
        price = product["price"]
        discount = product["discountPercentage"]
        product["final_price"] = round(price - (price * discount / 100), 2)

    # Convert to DataFrame
    actual_df = pd.DataFrame(products)
    actual_df.to_parquet(f"{path}{f_name}")

    logging.info(f"Written actual product data")

    return f"{path}{f_name}"


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


def scraper(
    url: str, expected_result_path: str, limit: int = 50, skip: int = 0
) -> Dict:
    """
    main function call
    :param url: String, api endpoint to be scraped
    :param expected_result_path: String, file path of expected product, *parquet
    :param limit: Integer, limit size for pagination
    :param skip: Integer, skip size to determine where to start next pagination
    :return: Dictionary object of analysis of result
    """
    logging.info(f"Starting scraper on Url: {url} ...")

    # Fetch actual data from API
    result = {}
    actual_data_path = fetch_actual_data(url, limit, skip)
    actual_df: pd.DataFrame = load_data(actual_data_path)
    logging.info("Loaded actual product data from parquet")

    # Load expected data from parquet file
    expected_df: pd.DataFrame = load_data(expected_result_path)
    logging.info("Loaded expected product data from parquet")

    # Analyze data and get the answers
    most_expensive_product, missing_products, price_matches = analyze_data(
        actual_df, expected_df
    )

    result["Most expensive product (actual)"] = most_expensive_product
    result["Missing products:"] = missing_products[
        ["id", "title_actual", "final_price_actual"]
    ].to_dict(orient="records")
    result["Number of rows where final price matches"] = price_matches

    return result


if __name__ == "__main__":
    api = "https://dummyjson.com/products"
    product_prices_result_path = "./data/product_prices_calculated.parquet"
    result = scraper(api, product_prices_result_path)
    print(json.dumps(result, indent=4))
