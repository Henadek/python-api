import asyncio
import json
import logging
import os
from typing import Dict, List

import httpx
import pandas as pd

from utils import analyze_data, load_data

logging.basicConfig(format="%(asctime)s -> %(message)s", level=logging.DEBUG)


async def generate_urls(base_url: str, limit: int = 50) -> List[str]:
    """
    Generate a list of URLs for each page based on the total number of products and limit.
    :param base_url: String, api endpoint to be scraped
    :param limit: Integer, limit size for pagination
    :return: List, generated list of URLs to be scraped
    """
    urls = []

    async with httpx.AsyncClient() as client:
        pre_flight = await fetch_data(client, f"{base_url}?limit=1")
    total_products = pre_flight["total"]

    for skip in range(0, total_products, limit):
        urls.append(f"{base_url}?limit={limit}&skip={skip}")

    return urls


async def fetch_data(client: httpx.AsyncClient, url: str) -> Dict:
    """
    Fetch data from a single URL asynchronously using httpx.
    :param client: AsyncClient, asynchronous HTTP client
    :param url: String, api endpoint to be scraped
    :return: Dictionary object of response data
    """
    response: httpx.Response = await client.get(url)
    response.raise_for_status()
    return response.json()


async def fetch_actual_data(urls: List[str]) -> str:
    """
    Fetch all product data concurrently using pre-generated API/URLs.
    :param urls: List, URLs to be scraped
    :return: String, filepath actual data is written out to
    """
    path, f_name = "./data/raw/", "raw_product_data.parquet"
    if not os.path.exists(path):
        os.mkdir(path)

    products = []

    async with httpx.AsyncClient() as client:
        # Create tasks for each URL in the list
        tasks = [fetch_data(client, url) for url in urls]

        # Wait for all tasks to complete
        results = await asyncio.gather(*tasks)

        # Process each result and collect products
        for data in results:
            products.extend(data["products"])

        # Calculate the final price for each product
        for product in products:
            price = product["price"]
            discount = product["discountPercentage"]
            product["final_price"] = round(price - (price * discount / 100), 2)

    # Convert to DataFrame
    actual_df = pd.DataFrame(products)
    actual_df.to_parquet(f"{path}{f_name}")

    logging.info(f"Written actual product data")

    return f"{path}{f_name}"


# def analyze_data(
#     actual_df: pd.DataFrame, expected_df: pd.DataFrame
# ) -> Tuple[str, pd.DataFrame, int]:
#     """
#     Compare expected df result to actual df result, with summary of vital information
#     :param actual_df: DataFrame, actual dataframe
#     :param expected_df: String, expected dataframe
#     :return: Tuple of [string, dataframe, integer] for most expensive product, missing products and count of product
#     price matches from actual to expected data
#     """
#     # Q1: What is the most expensive product according to actual data?
#     most_expensive_product: str = actual_df.loc[actual_df["final_price"].idxmax()][
#         "title"
#     ]
#
#     # Q2: What product is missing in expected data?
#     missing_products: pd.DataFrame = pd.merge(
#         actual_df,
#         expected_df,
#         how="left",
#         on="id",
#         indicator=True,
#         suffixes=("_actual", "_expected"),
#     )
#     missing_products = missing_products[missing_products["_merge"] == "left_only"]
#
#     # Q3: For how many rows final price matches between actual and expected data?
#     merged_df = pd.merge(
#         actual_df[["id", "final_price"]],
#         expected_df[["id", "final_price"]],
#         on="id",
#         suffixes=("_actual", "_expected"),
#     )
#     price_matches: int = int(
#         (merged_df["final_price_actual"] == merged_df["final_price_expected"]).sum()
#     )
#
#     logging.info(f"Completed Analysis of actual and expected product data")
#
#     return most_expensive_product, missing_products, price_matches


async def scraper(url: str, expected_result_path: str) -> Dict:
    """
    main function call
    :param url: String, api endpoint to be scraped
    :param expected_result_path: String, file path of expected product, *parquet
    :return: Dictionary object of analysis of result
    """
    logging.info(f"Starting scraper on Url: {url} ...")

    # Generate the list of URLs for pagination
    urls = await generate_urls(url)

    # Fetch actual data from API
    result = {}
    actual_data_path = await fetch_actual_data(urls)
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
    # Run the event loop
    api = "https://dummyjson.com/products"
    product_prices_result_path = "./data/product_prices_calculated.parquet"
    result = asyncio.run(scraper(api, product_prices_result_path))
    print(json.dumps(result, indent=4))
