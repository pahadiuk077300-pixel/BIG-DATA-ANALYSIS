# pyspark_analysis.py
main.py

"""
Big Data Analysis with PySpark
Cleaned Python script version for GitHub

Instructions:
- Install dependencies: pyspark
- Run using: python main.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Big Data Analysis") \
    .getOrCreate()

# Load Data (update path if needed)
data = spark.read.csv("data.csv", header=True, inferSchema=True)

# Show Schema
data.printSchema()

# Show first 5 rows
data.show(5)

# Basic Data Cleaning
data = data.dropna()

# Example Transformation
# (Modify column names based on your dataset)
if "salary" in data.columns:
    data = data.withColumn("salary_after_bonus", col("salary") * 1.1)

# Aggregation Example
if "department" in data.columns:
    result = data.groupBy("department").agg(avg("salary").alias("avg_salary"))
    result.show()

# Save Output
data.write.mode("overwrite").csv("output")

# Stop Spark Session
spark.stop()
