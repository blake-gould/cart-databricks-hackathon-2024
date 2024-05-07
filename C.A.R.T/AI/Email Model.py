# Databricks notebook source
!pip install openai

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from adventure_works.dim_customer

# COMMAND ----------

query = """
SELECT DISTINCT
  EmailAddress,
  FirstName,
  LastName,
  SalesOrderNumber,
  SalesOrderLineNumber,
  EnglishProductName,
  OrderQuantity,
  UnitPrice,
  '30% off 1 Item' as Offer,
  'Qx2V3mW8FjCt' as CouponCode,
  'Adam' as SalesRepName,
  'Adventure Works' as CompanyName
FROM
  adventure_works.v_fact_internet_sales
WHERE
  CustomerKey = '11025' AND SalesOrderNumber = 'SO43732'
ORDER BY
  SalesOrderNumber, SalesOrderLineNumber
"""

# Execute the SQL query
df = spark.sql(query)

# Convert the DataFrame to JSON
json_result = df.toJSON().collect()

# COMMAND ----------

json_result

# COMMAND ----------

json_result_str = " ".join(json_result)

# COMMAND ----------

json_result_str

# COMMAND ----------

from openai import OpenAI
import os
        
# DATABRICKS_TOKEN = os.environ.get('DATABRICKS_TOKEN')

# Prepare the prompt using the data
prompt = f"""
Generate a fully formatted and ready-to-send customer service email regarding a recent product return. The email content should include the following elements:
- Start with a professional and warm greeting, addressing the customer by their full name.
- Acknowledge the specific product returned, referencing the product name, sales order number, and line number.
- Express gratitude for the customer's patience and understanding during the return process.
- Detail the special offer provided, including the exact coupon code and instructions for how to use it.
- Provide the name and direct contact information of the sales representative assigned to their account for any further assistance.
- Conclude with a warm and engaging closing that invites further communication and reinforces the customer’s importance to the company.
- Include full company contact details at the end of the email.  The website is databricks.com

Ensure the tone is warm, professional, and engaging, aimed at ensuring customer satisfaction and encouraging future interactions.

Ensure your reponse contains only the email text, ready to send.

"""
        
client = OpenAI(
  api_key='****',
  base_url="https://****.cloud.databricks.com/serving-endpoints"
)
        
chat_completion = client.chat.completions.create(
  messages=[
  {
    "role": "system",
    "content": prompt
  },
  {
    "role": "user",
    "content": json_result_str
  }
  ],
  model="databricks-meta-llama-3-70b-instruct",
  max_tokens=500
)
        
print(chat_completion.choices[0].message.content)

# COMMAND ----------

