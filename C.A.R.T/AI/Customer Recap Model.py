# Databricks notebook source
!pip install openai

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view raw_data as
# MAGIC with rev as (
# MAGIC   select
# MAGIC     sum(SalesAmount) rev,
# MAGIC     customerkey
# MAGIC   from
# MAGIC     adventure_works.v_fact_internet_sales
# MAGIC   group by customerkey
# MAGIC )
# MAGIC
# MAGIC select
# MAGIC   a.*,
# MAGIC   b.purchase_prob,
# MAGIC   c.rev,
# MAGIC   d.score
# MAGIC from
# MAGIC   cart_databricks_hackaton_2024.prodds.customer_spend a
# MAGIC left join cart_databricks_hackaton_2024.prodds.customer_purchase b on a.customerkey = b.customerkey
# MAGIC left join rev c on a.customerkey = c.customerkey
# MAGIC left join prodds.customer_accessory_to_bike_score d on d.customerkey = a.customerkey

# COMMAND ----------

df = spark.table("raw_data")

# COMMAND ----------

from pyspark.sql.functions import percent_rank, col, when, lit
from pyspark.sql import Window

# Generic function to add rank and quartile columns to DataFrame
def add_quartile_columns(df, column_name, tiebreaker):
    windowSpec = Window.orderBy(col(column_name).desc(), col(tiebreaker).desc())
    rank_col = f"{column_name}_rank"
    quartile_col = f"{column_name}_quartile"

    df = df.withColumn(rank_col, percent_rank().over(windowSpec))
    df = df.withColumn(quartile_col, when(col(rank_col) <= 0.25, 1)
                                       .when(col(rank_col) <= 0.50, 2)
                                       .when(col(rank_col) <= 0.75, 3)
                                       .otherwise(4))
    return df

# Specific function for the score column with null handling and tiebreaker
def add_score_quartile_columns(df, tiebreaker):
    column_name = "score"
    
    # Handling nulls explicitly for score and assigning a quartile of 0
    df = df.withColumn(column_name, when(col(column_name).isNull(), 0).otherwise(col(column_name)))
    
    # Defining window specification for scoring with tiebreaker
    windowSpec = Window.orderBy(col(column_name).desc(), col(tiebreaker).desc())

    rank_col = f"{column_name}_rank"
    quartile_col = f"{column_name}_quartile"
    
    # Assign ranks and quartiles
    df = df.withColumn(rank_col, percent_rank().over(windowSpec))
    df = df.withColumn(quartile_col, when(col(column_name) == 0, 0)  # Assign 0 to originally null scores
                                     .when(col(rank_col) <= 0.25, 1)
                                     .when(col(rank_col) <= 0.50, 2)
                                     .when(col(rank_col) <= 0.75, 3)
                                     .otherwise(4))
    return df
    
# Applying the specific function for score
df = add_score_quartile_columns(df, "rev")

# Applying the generic function for other columns
columns_to_process = [('spend', 'rev'), ('purchase_prob', 'rev')]
for column, tiebreaker in columns_to_process:
    df = add_quartile_columns(df, column, tiebreaker)


# COMMAND ----------

df.createOrReplaceTempView("ranks")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS prodds.ai_customer_details;
# MAGIC CREATE TABLE prodds.ai_customer_details as
# MAGIC
# MAGIC WITH AvgMetrics AS (
# MAGIC     SELECT
# MAGIC         AVG(TotalOrders) AS AvgTotalOrders,
# MAGIC         AVG(TotalUnitsOrdered) AS AvgTotalUnitsOrdered,
# MAGIC         AVG(TotalSalesAmount) AS AvgTotalSalesAmount
# MAGIC     FROM (
# MAGIC         SELECT
# MAGIC             CustomerKey,
# MAGIC             COUNT(DISTINCT SalesOrderNumber) AS TotalOrders,
# MAGIC             SUM(OrderQuantity) AS TotalUnitsOrdered,
# MAGIC             SUM(SalesAmount) AS TotalSalesAmount
# MAGIC         FROM
# MAGIC             adventure_works.v_fact_internet_sales
# MAGIC         GROUP BY
# MAGIC             CustomerKey
# MAGIC     ) AS sub
# MAGIC ),
# MAGIC
# MAGIC customer_metrics AS (
# MAGIC SELECT
# MAGIC     sd.CustomerKey,
# MAGIC     COUNT(DISTINCT sd.SalesOrderNumber) AS TotalOrders,
# MAGIC     SUM(sd.OrderQuantity) AS TotalUnitsOrdered,
# MAGIC     AVG(sd.OrderQuantity) AS AvgUnitsOrdered,
# MAGIC     SUM(sd.SalesAmount) AS TotalSalesAmount,
# MAGIC     AVG(sd.SalesAmount) AS AvgSalesAmount,
# MAGIC     ROUND(first(am.AvgTotalOrders), 1) POP_AvgTotalOrders,
# MAGIC     ROUND(first(am.AvgTotalUnitsOrdered), 1) POP_AvgTotalUnitsOrdered,
# MAGIC     ROUND(first(am.AvgTotalSalesAmount), 1) POP_AvgTotalSalesAmount,
# MAGIC     MAX(sd.OrderDate) AS LatestOrderDate,
# MAGIC     MIN(sd.OrderDate) AS EarliestOrderDate,
# MAGIC     sd.FirstName,
# MAGIC     sd.LastName,
# MAGIC     sd.Gender,
# MAGIC     sd.HouseOwnerFlag,
# MAGIC     sd.NumberCarsOwned,
# MAGIC     sd.CommuteDistance,
# MAGIC     sd.YearlyIncome
# MAGIC FROM
# MAGIC     adventure_works.v_fact_internet_sales sd
# MAGIC CROSS JOIN AvgMetrics am
# MAGIC GROUP BY
# MAGIC     sd.CustomerKey,
# MAGIC     sd.FirstName,
# MAGIC     sd.LastName,
# MAGIC     sd.Gender,
# MAGIC     sd.HouseOwnerFlag,
# MAGIC     sd.NumberCarsOwned,
# MAGIC     sd.CommuteDistance,
# MAGIC     sd.YearlyIncome
# MAGIC ),
# MAGIC customer_ranks AS (
# MAGIC     SELECT
# MAGIC         customerkey,
# MAGIC         rev as Lifetime_Spend,
# MAGIC         spend_quartile,
# MAGIC         purchase_prob_quartile,
# MAGIC         score_quartile as bike_upsell_score_quartile
# MAGIC     FROM
# MAGIC         ranks
# MAGIC ),
# MAGIC bike_upsells AS (
# MAGIC     SELECT
# MAGIC         bike_name,
# MAGIC         upsell_1
# MAGIC     FROM
# MAGIC         prodds.bike_upsells
# MAGIC ),
# MAGIC joined AS (
# MAGIC     SELECT
# MAGIC         DISTINCT customerkey,
# MAGIC         upsell_1 AS upsell
# MAGIC     FROM
# MAGIC         adventure_works.v_fact_internet_sales a
# MAGIC         INNER JOIN bike_upsells b ON a.englishproductname = b.bike_name
# MAGIC ),
# MAGIC customer_upsell AS (
# MAGIC     SELECT
# MAGIC         customerkey,
# MAGIC         COLLECT_LIST(upsell) AS upsell_list
# MAGIC     FROM
# MAGIC         joined
# MAGIC     GROUP BY
# MAGIC         customerkey
# MAGIC )
# MAGIC SELECT
# MAGIC     cm.CustomerKey,
# MAGIC     cm.FirstName,
# MAGIC     cm.LastName,
# MAGIC     cm.TotalOrders,
# MAGIC     cm.TotalUnitsOrdered,
# MAGIC     cm.TotalSalesAmount,
# MAGIC     cm.POP_AvgTotalOrders,
# MAGIC     cm.POP_AvgTotalUnitsOrdered,
# MAGIC     cm.POP_AvgTotalSalesAmount,
# MAGIC     cm.LatestOrderDate,
# MAGIC     cm.EarliestOrderDate,
# MAGIC     cm.Gender,
# MAGIC     cm.HouseOwnerFlag,
# MAGIC     cm.NumberCarsOwned,
# MAGIC     cm.CommuteDistance,
# MAGIC     cm.YearlyIncome,
# MAGIC     cr.Lifetime_Spend,
# MAGIC     cr.spend_quartile future_spend_prediction_quartile,
# MAGIC     cr.purchase_prob_quartile future_purchase_prob_quartile,
# MAGIC     cr.bike_upsell_score_quartile,
# MAGIC     cu.upsell_list
# MAGIC FROM
# MAGIC     customer_metrics cm
# MAGIC LEFT JOIN
# MAGIC     customer_ranks cr ON cm.CustomerKey = cr.customerkey
# MAGIC LEFT JOIN
# MAGIC     customer_upsell cu ON cm.CustomerKey = cu.customerkey
# MAGIC LEFT JOIN
# MAGIC     adventure_works.dim_customer dc ON cm.CustomerKey = dc.customerkey

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from prodds.ai_customer_details

# COMMAND ----------

json_result