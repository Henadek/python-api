import requests
import pandas as pd


# Fetch actual product data from the API
def get_actual_data():
    url = "https://dummyjson.com/products"
    params = {"limit": 194}

    response = requests.get(url, params=params)

    if response.status_code != 200:
        raise Exception(f"Error fetching data: {response.status_code}")

    products = response.json().get('products', [])

    actual_data = []

    # Looping through each product and calculating final price
    for product in products:
        price = product.get('price', 0)
        discount = product.get('discountPercentage', 0)

        final_price = price - (price * discount / 100)

        actual_data.append({
            "id": product['id'],
            "title": product['title'],
            "price": price,
            "discountPercentage": discount,
            "final_price": round(final_price, 2)  # round to 2 decimal places
        })

    df = pd.DataFrame(actual_data)

    # Optional
    df.to_csv('actual_product_data.csv', index=False)

    return df


# Fetch actual product data
actual_product_data = get_actual_data()
print(
    actual_product_data.head()
)  # Preview the first few rows of the actual data
