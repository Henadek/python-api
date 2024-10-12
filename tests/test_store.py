import numpy as np
import pytest
from httpx import AsyncClient

from utils import load_data

expected_data_path = "./data/product_prices_calculated.parquet"
actual_data_path = "./data/raw/raw_product_data.parquet"
total_product_count = 0


@pytest.fixture()
def actual_data():
    df = load_data(actual_data_path)
    return df


@pytest.fixture()
def expected_data():
    df = load_data(expected_data_path)
    return df


class TestStore:

    @pytest.mark.asyncio
    async def test_store_api(self):
        """check if endpoint works"""
        api = "https://dummyjson.com/products"
        global total_product_count

        async with AsyncClient() as client:
            pre_flight = await client.get(f"{api}?limit=1")
            total_product_count = pre_flight.json()["total"]

        assert pre_flight.status_code == 200

    def test_product_count(self, actual_data):
        """Amount of products returned by this API is 194 (not 100!)"""
        global total_product_count
        assert len(actual_data) == 194
        assert len(actual_data) == total_product_count

    def test_expected_data_final_price(self, expected_data):
        """rounded to 2 decimal places"""
        final_prices = expected_data["final_price"]

        # Check if all final prices are rounded to 2 decimal places
        for price in final_prices:
            if isinstance(price, float | int) and not np.isnan(
                price
            ):  # TODO: verify if NaN should be validated or not
                assert round(price, 2) == price
