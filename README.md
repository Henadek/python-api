## Python self-assessment task 

This Python task involves fetching data from API, JSON data handling, working with parquet files
and performing simple calculations. Also, it requires basic git skills to upload the results. 
You will need to compare two product lists - actual data from url and expected data stored in parquet file.

### Actual data
Get products data from `https://dummyjson.com/products`
- **Total amount of products > 100** (use query parameters in the above path)
- Final price for each product can be calculated using 2 fields from the response - "price" and "discountPercentage"
- **Please, do not overwrite expected data with actual data!** Use different path if you want to save actual data.

### Expected data 
Stored in `./data/product_prices_calculated.parquet`
- Final price in expected data is rounded to 2 decimal places

### Questions
1. What product is the most expensive according to actual data?
2. What product is missing in expected data comparing with actual data?
3. For how many rows final price in expected data matches with calculated price from actual data?

### Task
1. Clone this repo (not fork)
2. Create separate feature branch
3. Write your Python code that answers the above questions
4. Please, run your code before creating PR - it should pass without errors
5. Create PR into the master branch
6. Share the PR link with our HR

**Pay attention, that your code should follow PEP8 standards.** 
- It should be easy to read, effective and explicit.
- It's recommended to create separate functions for each step of your solution (modular programming). 
Like:
```
get_actual_data()
upload_expected_data()
find_most_expensive_product()
...
```
Feel free to use your own naming. It's just the example.

__If you have technical questions, feel free to reach out - dbulychev@jaxel.com or t.me/dena_den__
