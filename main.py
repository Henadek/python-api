from scripts.fetch_actual_data import get_actual_data
from scripts.load_expected_data import load_expected_data
from scripts.compare_data import (
    find_most_expensive_product,
    find_missing_products,
    count_matching_prices
)


def main():
    # Fetch actual product data
    actual_product_data = get_actual_data()

    # Load expected product data
    expected_product_data = load_expected_data()

    # SFind the most expensive product in actual data
    most_expensive = find_most_expensive_product(actual_product_data)
    print("\n--- Most Expensive Product ---")
    print(f"Product: {most_expensive['title']}")
    print(f"Price: {most_expensive['final_price']}\n")

    # Find missing products in expected data
    missing_products = find_missing_products(
        actual_product_data, expected_product_data
    )
    print("--- Missing Products in Expected Data ---")
    if len(missing_products) > 0:
        print(f"Number of missing products: {len(missing_products)}")
        print(missing_products[['id', 'title']].to_string(index=False))
    else:
        print("No products are missing.\n")

    # Count how many rows have matching final prices
    matching_rows = count_matching_prices(
        actual_product_data, expected_product_data
    )
    print(
        "\n--- Price Comparison ---"
        f"\nNumber of matching rows (final prices): {matching_rows} "
        f"out of {len(actual_product_data)}"
    )


if __name__ == "__main__":
    main()
