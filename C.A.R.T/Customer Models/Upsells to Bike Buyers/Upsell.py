# Databricks notebook source
# MAGIC %sql
# MAGIC use adventure_works

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW raw_data as
# MAGIC SELECT
# MAGIC   salesordernumber transaction_id,
# MAGIC   productkey,
# MAGIC   COALESCE(EnglishProductCategoryName, "Not Bike") category
# MAGIC FROM
# MAGIC   v_fact_reseller_sales

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   cats,
# MAGIC   count(1) count
# MAGIC from
# MAGIC   (
# MAGIC     select
# MAGIC       count(distinct category) cats,
# MAGIC       transaction_id
# MAGIC     from
# MAGIC       raw_data
# MAGIC     group by
# MAGIC       transaction_id
# MAGIC   )
# MAGIC group by
# MAGIC   cats

# COMMAND ----------

df = spark.table("raw_data")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# Filter to get only transactions that include at least one Bike
bike_transactions = df.filter(df.category == "Bikes").select("transaction_id").distinct()

# Join this back to the original DataFrame to get all products in these transactions
df_joined = df.join(bike_transactions, "transaction_id")

# Filter out the bikes from the joined DataFrame to avoid counting them
non_bike_products = df_joined.filter(df_joined.category != "Bikes")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Perform a self join on the DataFrame to find pairs of products bought in the same transaction,
# specifically pairing bikes with non-bike products.
paired_products = df_joined.filter(df_joined.category == "Bikes").alias("bikes") \
    .join(non_bike_products.alias("products"), "transaction_id") \
    .select(
        F.col("bikes.productkey").alias("bike_key"),  # Select and rename the bike's product key
        F.col("products.productkey").alias("product_key"),  # Select and rename the paired product's key
        F.col("products.category").alias("product_category")  # Select and rename the paired product's category
    )

# Group by bike key, paired product key, and product category to count how often each product is bought
# with each bike. This helps in determining the most commonly purchased products with bikes.
product_counts = paired_products.groupBy("bike_key", "product_key", "product_category") \
    .count()  # Count occurrences of each product pair

# Define a window specification to rank products for each bike within each product category.
# Products are ordered by the count in descending order to prioritize products most frequently bought with bikes.
window_spec = Window.partitionBy("bike_key", "product_category").orderBy(F.desc("count"))

# Rank products within each partition (bike_key and product_category) and filter to get the top 3.
# This step ensures we only consider the top three most frequently bought products from different categories for each bike.
top_products = product_counts.withColumn("rank", F.rank().over(window_spec)) \
    .filter(F.col("rank") <= 3)  # Filter to keep only top 3 ranked products

# COMMAND ----------

# Define a window specification for ranking products within each bike and category
window_spec = Window.partitionBy("bike_key", "product_category").orderBy(F.desc("count"))

# Rank products within each bike and category, and filter to retain only top 3
top_products = product_counts.withColumn("rank", F.rank().over(window_spec)) \
    .filter(F.col("rank") <= 3)

# Group by bike_key and aggregate product information into lists
grouped_products = top_products.groupBy("bike_key").agg(
    F.collect_list("product_key").alias("product_keys"),  # List of product keys
    F.collect_list("product_category").alias("product_categories"),  # Corresponding categories
    F.collect_list("rank").alias("ranks")  # Corresponding ranks
)

# Select and transform the aggregated data into a structured format:
# Extract the top three products (if available) into separate columns
final_output = grouped_products.select(
    "bike_key",
    F.when(F.size("product_keys") >= 1, F.col("product_keys")[0]).alias("Top_Product_1"),  # First top product
    F.when(F.size("product_keys") >= 2, F.col("product_keys")[1]).alias("Top_Product_2"),  # Second top product
    F.when(F.size("product_keys") >= 3, F.col("product_keys")[2]).alias("Top_Product_3")  # Third top product
)

# COMMAND ----------

final_output.createOrReplaceTempView("output")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS cart_databricks_hackaton_2024.prodds.bike_upsells;
# MAGIC CREATE TABLE cart_databricks_hackaton_2024.prodds.bike_upsells as
# MAGIC select
# MAGIC   b.EnglishProductName bike_name,
# MAGIC   c.EnglishProductName upsell_1,
# MAGIC   d.EnglishProductName upsell_2,
# MAGIC   e.EnglishProductName upsell_3
# MAGIC from
# MAGIC   output a
# MAGIC left join dim_product b on a.bike_key = b.productkey
# MAGIC left join dim_product c on a.Top_Product_1 = c.productkey
# MAGIC left join dim_product d on a.Top_Product_2 = d.productkey
# MAGIC left join dim_product e on a.Top_Product_3 = e.productkey