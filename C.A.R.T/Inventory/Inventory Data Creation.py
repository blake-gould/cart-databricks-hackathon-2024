# Databricks notebook source
# MAGIC %pip install prophet

# COMMAND ----------

# MAGIC %sql
# MAGIC USE adventure_works;

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct englishdescription from adventure_works.dim_product

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW raw_data as
# MAGIC -- Find the first sale date for each product
# MAGIC WITH FirstSaleDate AS (
# MAGIC   SELECT
# MAGIC     ProductKey,
# MAGIC     MIN(FullDateAlternateKey) as FirstSaleDate
# MAGIC   FROM v_fact_internet_sales
# MAGIC   GROUP BY ProductKey
# MAGIC ),
# MAGIC
# MAGIC MaxDate AS (
# MAGIC   SELECT MAX(FullDateAlternateKey) as MaxDate
# MAGIC   FROM v_fact_internet_sales
# MAGIC ),
# MAGIC
# MAGIC -- Generate a series of dates for each product from its first sale date to the max date found in the fact table
# MAGIC DateSeries AS (
# MAGIC   SELECT
# MAGIC     f.ProductKey,
# MAGIC     d.FullDateAlternateKey as ds
# MAGIC   FROM FirstSaleDate f
# MAGIC   CROSS JOIN dim_date d
# MAGIC   JOIN MaxDate m
# MAGIC     ON d.FullDateAlternateKey >= f.FirstSaleDate AND d.FullDateAlternateKey <= m.MaxDate
# MAGIC ),
# MAGIC
# MAGIC -- Sum sales per product per day, using LEFT JOIN to include all dates
# MAGIC ProductSales AS (
# MAGIC   SELECT
# MAGIC     ds.ProductKey,
# MAGIC     ds.ds,
# MAGIC     COALESCE(SUM(v.OrderQuantity), 0) as y
# MAGIC   FROM DateSeries ds
# MAGIC   LEFT JOIN v_fact_internet_sales v
# MAGIC     ON ds.ds = v.FullDateAlternateKey AND ds.ProductKey = v.ProductKey
# MAGIC   GROUP BY ds.ProductKey, ds.ds
# MAGIC )
# MAGIC
# MAGIC -- Select the final result set
# MAGIC SELECT * FROM ProductSales
# MAGIC ORDER BY ProductKey, ds;

# COMMAND ----------

from prophet import Prophet
import pandas as pd

# COMMAND ----------

# Load data
df = spark.sql("select * from raw_data").toPandas()
df['ds'] = pd.to_datetime(df['ds'])
df['ProductKey'] = df['ProductKey'].astype(str)

# Dictionary to store forecasts for each product
all_forecasts = {}

# Iterate over each unique product key
for product_key in df['ProductKey'].unique():
    # Filter the DataFrame for the current product key
    product_df = df[df['ProductKey'] == product_key]

    # Initialize and fit a Prophet model
    model = Prophet()
    model.fit(product_df[['ds', 'y']])

    # Create a DataFrame for future predictions, extending 28 days into the future
    future = model.make_future_dataframe(periods=28, include_history=False)

    # Make predictions
    forecast = model.predict(future)

    # Set negative forecasts to zero and round to nearest whole number
    forecast['yhat'] = forecast['yhat'].clip(lower=0).round(0)

    # Group by week and sum the demand forecast
    forecast['week'] = forecast['ds'].dt.isocalendar().week
    weekly_forecast = forecast.groupby('week')['yhat'].sum().reset_index()

    # Ensure we have 4 weeks of data
    expected_weeks = [forecast['week'].min() + i for i in range(4)]
    weekly_forecast = weekly_forecast[weekly_forecast['week'].isin(expected_weeks)]

    # Store the weekly sum in the dictionary
    all_forecasts[product_key] = weekly_forecast.set_index('week')['yhat'].reindex(expected_weeks, fill_value=0).reset_index()

# Combine all forecasts into a single DataFrame
combined_forecasts = pd.concat(all_forecasts, names=['ProductKey', 'Index']).reset_index(level=0)

# Rename and reformat the weekly columns
combined_forecasts = combined_forecasts.pivot(index='ProductKey', columns='week', values='yhat')
combined_forecasts.columns = [f'Week{i+1}Demand' for i in range(4)]

# Convert the combined DataFrame back to a Spark DataFrame
spark_df = spark.createDataFrame(combined_forecasts.reset_index())

# Show result or save to table, as needed
spark_df.display()

# COMMAND ----------

import openai
import numpy as np

def get_embedding(text):
    response = openai.Embedding.create(
        model="text-embedding-ada-002",
        input=text
    )
    return response['data']['embedding']

def cosine_similarity(vec1, vec2):
    return np.dot(vec1, vec2) / (np.linalg.norm(vec1) * np.linalg.norm(vec2))

def find_top_matches(descriptions, query, top_n=3):
    query_emb = get_embedding(query)
    scores = []
    
    for desc in descriptions:
        desc_emb = get_embedding(desc)
        score = cosine_similarity(query_emb, desc_emb)
        scores.append((desc, score))
    
    # Sort by score in descending order and return the top N descriptions
    scores.sort(key=lambda x: x[1], reverse=True)
    return scores[:top_n]

# Example usage:
descriptions = [
    "Lightweight butted aluminum frame provides a more upright riding position for a trip around town. Our ground-breaking design provides optimum comfort.",
    "Sturdy steel frame with a classic design and a smooth ride. Perfect for daily commutes and leisurely weekend rides.",
    "Advanced carbon fiber road bike with an aggressive geometry for racing at the highest levels. Extremely lightweight and responsive."
]

query = "lightweight, aluminum, upright, comfortable"
top_matches = find_top_matches(descriptions, query)
for desc, score in top_matches:
    print(f"Description: {desc}\nScore: {score}\n")


# COMMAND ----------

spark_df.write.mode("overwrite").saveAsTable("cart_databricks_hackaton_2024.prodds.product_forecasts")

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   ProductKey,  
# MAGIC   ROUND(sum(Profit) / sum(SalesAmount), 2) as Margin_Pct
# MAGIC from
# MAGIC   adventure_works.v_fact_internet_sales
# MAGIC group by all

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS cart_databricks_hackaton_2024.prodds.product_inventory_status;
# MAGIC CREATE TABLE cart_databricks_hackaton_2024.prodds.product_inventory_status as 
# MAGIC WITH DemandData AS (
# MAGIC   SELECT
# MAGIC     a.ProductKey,
# MAGIC     UnitsBalance,
# MAGIC     LeadTimeWeeks,
# MAGIC     Week1Demand,
# MAGIC     Week2Demand,
# MAGIC     Week3Demand,
# MAGIC     Week4Demand,
# MAGIC     (
# MAGIC       Week1Demand + Week2Demand + Week3Demand + Week4Demand
# MAGIC     ) AS TotalForecastedDemand
# MAGIC   FROM
# MAGIC     v_fact_product_inventory a
# MAGIC     inner join prodds.product_forecasts b on a.productkey = b.productkey
# MAGIC ),
# MAGIC RankedProducts AS (
# MAGIC   SELECT
# MAGIC     *,
# MAGIC     ROW_NUMBER() OVER (
# MAGIC       ORDER BY
# MAGIC         TotalForecastedDemand DESC
# MAGIC     ) AS Rank,
# MAGIC     COUNT(*) OVER () AS TotalCount
# MAGIC   FROM
# MAGIC     DemandData
# MAGIC ),
# MAGIC StockPolicy AS (
# MAGIC   SELECT
# MAGIC     ProductKey,
# MAGIC     UnitsBalance,
# MAGIC     LeadTimeWeeks,
# MAGIC     Week1Demand,
# MAGIC     Week2Demand,
# MAGIC     Week3Demand,
# MAGIC     Week4Demand,
# MAGIC     TotalForecastedDemand,
# MAGIC     GREATEST(CASE
# MAGIC       WHEN Rank <= TotalCount * 0.2 -- Top 20%
# MAGIC       THEN CEIL(TotalForecastedDemand * 1.15) - UnitsBalance -- Adding 15% safety stock, adjusting with current stock
# MAGIC       ELSE TotalForecastedDemand + 1 - UnitsBalance -- Just demand forecast plus one unit, adjusting with current stock
# MAGIC     END, 0) AS OrderQuantity
# MAGIC   FROM
# MAGIC     RankedProducts
# MAGIC ),
# MAGIC
# MAGIC Margin as (
# MAGIC   select
# MAGIC   ProductKey,  
# MAGIC   ROUND(sum(Profit) / sum(SalesAmount), 2) as Margin_Pct
# MAGIC   from
# MAGIC     adventure_works.v_fact_internet_sales
# MAGIC   group by all
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   a.*,
# MAGIC   CASE
# MAGIC     WHEN UnitsBalance > 0 THEN 1
# MAGIC     ELSE 0
# MAGIC   END AS HasStock,
# MAGIC   CASE
# MAGIC     WHEN OrderQuantity > 0 THEN 1
# MAGIC     ELSE 0
# MAGIC   END AS NeedsOrder,
# MAGIC   CASE
# MAGIC     WHEN UnitsBalance > 0 THEN 0
# MAGIC     ELSE LeadTimeWeeks
# MAGIC   END AS WeeksUntilStockAvailable,
# MAGIC   b.Margin_Pct
# MAGIC FROM
# MAGIC   StockPolicy a
# MAGIC LEFT JOIN Margin b on a.ProductKey = b.ProductKey