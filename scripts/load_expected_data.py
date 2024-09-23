import pandas as pd


# Load expected product data from parquet file
def load_expected_data():
    file_path = './data/product_prices_calculated.parquet'

    # Load the parquet file into a DataFrame
    try:
        expected_data = pd.read_parquet(
            file_path, engine='pyarrow'
        )
    except Exception as e:
        raise Exception(f"Error loading paruqet file: {e}")

    # Ensure the 'final_price' column is rounded to 2 decimal places
    expected_data['final_price'] = expected_data['final_price'].round(2)

    return expected_data


# Load expected product data
expected_product_data = load_expected_data()
print(
    expected_product_data.head()
)  # Preview the first few rows of the expected data
