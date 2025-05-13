from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def get_product_category_pairs(products_df, categories_df, product_category_df):
    """
    Returns a DataFrame containing all product-category pairs and products without categories.
    
    Args:
        products_df (DataFrame): DataFrame with products (must have 'product_id' and 'product_name' columns)
        categories_df (DataFrame): DataFrame with categories (must have 'category_id' and 'category_name' columns)
        product_category_df (DataFrame): DataFrame with product-category relationships (must have 'product_id' and 'category_id' columns)
    
    Returns:
        DataFrame: Contains all product-category pairs and products without categories
    """
    # Join products with their categories
    product_category_pairs = (product_category_df
        .join(products_df, on='product_id', how='inner')
        .join(categories_df, on='category_id', how='inner')
        .select('product_name', 'category_name'))
    
    # Get products without categories
    products_without_categories = (products_df
        .join(product_category_df, on='product_id', how='left_anti')
        .select('product_name')
        .withColumn('category_name', col('product_name')))  # Duplicate product_name as category_name for products without categories
    
    # Union both results
    result = product_category_pairs.union(products_without_categories)
    
    return result

# Example usage:
if __name__ == "__main__":
    # Create Spark session
    spark = SparkSession.builder \
        .appName("ProductCategoryJoin") \
        .getOrCreate()
    
    # Example data
    products_data = [
        (1, "Product A"),
        (2, "Product B"),
        (3, "Product C"),
        (4, "Product D")
    ]
    
    categories_data = [
        (1, "Category X"),
        (2, "Category Y"),
        (3, "Category Z")
    ]
    
    product_category_data = [
        (1, 1),  # Product A - Category X
        (1, 2),  # Product A - Category Y
        (2, 1),  # Product B - Category X
        (3, 3)   # Product C - Category Z
    ]
    
    # Create DataFrames
    products_df = spark.createDataFrame(products_data, ["product_id", "product_name"])
    categories_df = spark.createDataFrame(categories_data, ["category_id", "category_name"])
    product_category_df = spark.createDataFrame(product_category_data, ["product_id", "category_id"])
    
    # Get results
    result_df = get_product_category_pairs(products_df, categories_df, product_category_df)
    
    # Show results
    result_df.show() 