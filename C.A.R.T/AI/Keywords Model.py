# Databricks notebook source
!pip install openai

# COMMAND ----------

# MAGIC %sql
# MAGIC use adventure_works;

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   *
# MAGIC FROM
# MAGIC   dim_product a
# MAGIC   LEFT JOIN dim_product_subcategory b on a.ProductSubcategoryKey = b.ProductSubcategoryKey
# MAGIC   LEFT JOIN dim_product_category c on c.ProductCategoryKey = b.ProductCategoryKey
# MAGIC where
# MAGIC   c.EnglishProductCategoryName = 'Bikes'
# MAGIC   and englishdescription is not null

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS prodds.bike_keywords;
# MAGIC CREATE TABLE prodds.bike_keywords as
# MAGIC WITH refined_prompts AS (
# MAGIC SELECT
# MAGIC     a.productkey,
# MAGIC     EnglishDescription AS description,
# MAGIC     size,
# MAGIC     weight,
# MAGIC     'Given the following product description, identify and list the key features and attributes in a comma-separated format. Do not include any other text, except the list: "' || description || ', Size: ' || size || ', Weight: ' || weight || '"' AS prompt
# MAGIC   FROM
# MAGIC     dim_product a
# MAGIC     LEFT JOIN dim_product_subcategory b on a.ProductSubcategoryKey = b.ProductSubcategoryKey
# MAGIC     LEFT JOIN dim_product_category c on c.ProductCategoryKey = b.ProductCategoryKey
# MAGIC   where
# MAGIC     c.EnglishProductCategoryName = 'Bikes' and englishdescription is not null
# MAGIC ), ai_responses AS (
# MAGIC   SELECT
# MAGIC     productkey,
# MAGIC     description,
# MAGIC     ai_query('databricks-meta-llama-3-70b-instruct', prompt) AS refined_description
# MAGIC   FROM
# MAGIC     refined_prompts
# MAGIC )
# MAGIC SELECT
# MAGIC   productkey,
# MAGIC   description,
# MAGIC   refined_description
# MAGIC FROM
# MAGIC   ai_responses

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from prodds.bike_keywords

# COMMAND ----------

# MAGIC %pip install databricks-vectorsearch

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import os
from databricks.vector_search.client import VectorSearchClient
import pandas as pd

vsc = VectorSearchClient(
  workspace_url='https://****.cloud.databricks.com',
  personal_access_token = '****'
)

index = vsc.get_index(endpoint_name="bike_keyword_search", index_name="cart_databricks_hackaton_2024.prodds.bike_keywords_index")

json_data = index.similarity_search(num_results=10, columns=["productkey", "refined_description"], query_text="lightweight")

df = pd.DataFrame(json_data['result']['data_array'], columns=[col['name'] for col in json_data['manifest']['columns']])

# COMMAND ----------

spark_df = spark.createDataFrame(df)
spark_df.createOrReplaceTempView("json_results")

df_inv = spark.sql("""
                   select p.productkey, p.englishproductname, p.EnglishDescription,
                            i.HasStock, i.WeeksUntilStockAvailable, i.Margin_Pct
                   
                    FROM json_results js

                    LEFT JOIN adventure_works.dim_product p
                        ON js.productkey = p.productkey

                    LEFT JOIN prodds.product_inventory_status i
                        ON js.productkey = i.productkey
                    """
                    )

display(df_inv)

# COMMAND ----------

import requests
import time

DATABRICKS_HOST = "https://****.cloud.databricks.com" 
TOKEN = "****"

# Endpoint to trigger a notebook job
endpoint = f"{DATABRICKS_HOST}/api/2.0/jobs/runs/submit"

# Headers for authentication
headers = {
    'Authorization': f'Bearer {TOKEN}'
}

# Endpoint to trigger a notebook job and to get job output
submit_endpoint = f"{DATABRICKS_HOST}/api/2.0/jobs/runs/submit"
output_endpoint = f"{DATABRICKS_HOST}/api/2.0/jobs/runs/get-output"

# Data for the API call
data = {
    "run_name": "Run from API",
    "existing_cluster_id": "****",
    "notebook_task": {
        "notebook_path": "/Workspace/Shared/AI/External API Call",
        "base_parameters": {
            "Search_text": "lightweight"  # Pass the value to the notebook widget
        }
    }
}

# Start the notebook job
response = requests.post(submit_endpoint, headers=headers, json=data)
if response.status_code == 200:
    run_id = response.json()['run_id']
    print("Notebook job started successfully. Run ID:", run_id)
else:
    print("Failed to start notebook job. Status Code:", response.status_code)
    print("Message:", response.json())
    exit()

# Function to check job status
def check_job_status(run_id):
    status_response = requests.get(f"{DATABRICKS_HOST}/api/2.0/jobs/runs/get?run_id={run_id}", headers=headers)
    status_response_json = status_response.json()
    return status_response_json['state']['life_cycle_state']

# Polling job status until it is terminated
while True:
    job_status = check_job_status(run_id)
    if job_status in ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"]:
        break
    time.sleep(1)  # Check every 1 seconds

# Fetch job output if job completed successfully
if job_status == "TERMINATED":
    output_response = requests.get(f"{output_endpoint}?run_id={run_id}", headers=headers)
    if output_response.status_code == 200:
        notebook_output = output_response.json().get('notebook_output', {}).get('result', 'No output found')
        print("Notebook output:", notebook_output)
    else:
        print("Failed to fetch notebook output. Status Code:", output_response.status_code)
        print("Message:", output_response.json())
else:
    print("Job did not complete successfully. Final status:", job_status)

# COMMAND ----------

