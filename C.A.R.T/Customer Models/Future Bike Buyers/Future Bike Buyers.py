# Databricks notebook source
# MAGIC %sql
# MAGIC use adventure_works

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view non_bike_buyers as
# MAGIC SELECT distinct
# MAGIC   a.customerkey,
# MAGIC   a.birthdate,
# MAGIC   a.maritalstatus,
# MAGIC   a.Gender,
# MAGIC   a.YearlyIncome,
# MAGIC   a.TotalChildren,
# MAGIC   a.NumberChildrenAtHome,
# MAGIC   a.EnglishEducation,
# MAGIC   a.EnglishOccupation,
# MAGIC   a.HouseOwnerFlag,
# MAGIC   a.NumberCarsOwned,
# MAGIC   a.CommuteDistance
# MAGIC FROM
# MAGIC   v_fact_internet_sales a
# MAGIC WHERE NOT EXISTS (
# MAGIC   SELECT 1
# MAGIC   FROM v_fact_internet_sales b
# MAGIC   WHERE b.customerkey = a.customerkey
# MAGIC     AND b.EnglishProductCategoryName = 'Bikes'
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view bike_buyers as
# MAGIC SELECT DISTINCT
# MAGIC   a.customerkey,
# MAGIC   a.birthdate,
# MAGIC   a.maritalstatus,
# MAGIC   a.Gender,
# MAGIC   a.YearlyIncome,
# MAGIC   a.TotalChildren,
# MAGIC   a.NumberChildrenAtHome,
# MAGIC   a.EnglishEducation,
# MAGIC   a.EnglishOccupation,
# MAGIC   a.HouseOwnerFlag,
# MAGIC   a.NumberCarsOwned,
# MAGIC   a.CommuteDistance
# MAGIC FROM
# MAGIC   v_fact_internet_sales a
# MAGIC WHERE
# MAGIC   EnglishProductCategoryName = 'Bikes'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bike_buyers

# COMMAND ----------

from pyspark.sql.functions import lit

# Load data
bike_buyers = spark.table("bike_buyers")
non_bike_buyers = spark.table("non_bike_buyers")

# Add label column
bike_buyers = bike_buyers.withColumn("BikeBuyer", lit(1))
non_bike_buyers = non_bike_buyers.withColumn("BikeBuyer", lit(0))

# Combine the data
df = bike_buyers.union(non_bike_buyers)

# COMMAND ----------

from pyspark.sql.functions import year, month, dayofmonth, current_date, datediff
from pyspark.ml.feature import StringIndexer, VectorAssembler

# Calculate age from birthdate
df = df.withColumn("Age", datediff(current_date(), df.birthdate) / 365)

# Indexing categorical columns
categoricalColumns = ["maritalstatus", "Gender", "EnglishEducation", "EnglishOccupation", "CommuteDistance", "HouseOwnerFlag"]
indexers = [StringIndexer(inputCol=c, outputCol=c+"_indexed").fit(df) for c in categoricalColumns]

# Assembling a feature vector
assembler = VectorAssembler(
    inputCols=[c+"_indexed" for c in categoricalColumns] + 
    ["YearlyIncome", "TotalChildren", "NumberChildrenAtHome", "NumberCarsOwned", "Age"],
    outputCol="features"
)

# COMMAND ----------

from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline

# Define the classifier
classifier = LogisticRegression(featuresCol="features", labelCol="BikeBuyer")

# Build the pipeline
pipeline = Pipeline(stages=indexers + [assembler, classifier])

# Train the model
model = pipeline.fit(df)

# COMMAND ----------



# COMMAND ----------

from pyspark.sql.functions import col, udf
from pyspark.sql.types import FloatType

# UDF to extract the probability of the positive class
extract_probability = udf(lambda x: float(x[1]), FloatType())

# Transform the model and filter out non-bike buyers
predictions = model.transform(df.filter("BikeBuyer == 0"))

# Select customerkey and the probability of being a bike buyer
probability_df = predictions.select(
    "customerkey",
    extract_probability("probability").alias("score")
).orderBy("score", ascending=False)

# Show the DataFrame (if needed, or you can use .display() in Databricks)
probability_df.display()

# COMMAND ----------

probability_df.write.mode("overwrite").saveAsTable("cart_databricks_hackaton_2024.prodds.customer_accessory_to_bike_score")