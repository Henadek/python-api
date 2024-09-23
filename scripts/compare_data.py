import pandas as pd


# Find the most expensive product in the actual data
def find_most_expensive_product(actual_data):
    most_expensive_product = actual_data.loc[
        actual_data['final_price'].idxmax()
    ]
    return most_expensive_product


# Identify products missing in expected data
def find_missing_products(actual_data, expected_data):
    expected_ids = set(expected_data['id'])
    missing_products = actual_data[
        ~actual_data['id'].isin(expected_ids)
    ]
    return missing_products


# Compare final prices and count matching rows
def count_matching_prices(actual_data, expected_data):
    merged_data = pd.merge(
        actual_data[['id', 'final_price']],
        expected_data[['id', 'final_price']],
        on='id',
        suffixes=('_actual', '_expected')
    )

    matching_rows = (
        merged_data['final_price_actual'] ==
        merged_data['final_price_expected']
    ).sum()
    return matching_rows
