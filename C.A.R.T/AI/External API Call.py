# Databricks notebook source
dbutils.widgets.text("Search_text", "heavyweight")
search_text = dbutils.widgets.get("Search_text")
print(search_text)

# COMMAND ----------

# MAGIC %md
# MAGIC install databricks-vectorsearch
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from databricks.vector_search.client import VectorSearchClient
import pandas as pd
import json

vsc = VectorSearchClient()

index = vsc.get_index(endpoint_name="bike_keyword_search", index_name="cart_databricks_hackaton_2024.prodds.bike_keywords_index")

json_data = index.similarity_search(num_results=10, columns=["productkey", "refined_description"], query_text=search_text)

df = pd.DataFrame(json_data['result']['data_array'], columns=[col['name'] for col in json_data['manifest']['columns']])

# COMMAND ----------

spark_df = spark.createDataFrame(df)
spark_df.createOrReplaceTempView("json_results")

df_inv = spark.sql("""
                   select p.englishproductname as `Bike Name`, p.Weight, p.Size,
                            i.HasStock as `Has Stock`, i.Margin_Pct Margin, js.score as `Similarity`
                   
                    FROM json_results js

                    LEFT JOIN adventure_works.dim_product p
                        ON js.productkey = p.productkey

                    LEFT JOIN prodds.product_inventory_status i
                        ON js.productkey = i.productkey

                    ORDER BY js.score desc
                    """)

# COMMAND ----------

pd_df = df_inv.toPandas()

# COMMAND ----------

result = pd_df.to_json()

# COMMAND ----------

dbutils.notebook.exit(result)