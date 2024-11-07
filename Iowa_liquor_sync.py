# Databricks notebook source
dbutils.fs.mounts()

# COMMAND ----------

# Define the path to the JSON file
input_path = "/mnt/incremental/bronze/incremental/daily_sync.json"

# Read the JSON file with the multiline option enabled
df = spark.read.option("multiline", "true").json(input_path)

# Display the schema and some sample records
df.printSchema()



# COMMAND ----------

df.show(5)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, to_date

# Read the data (assuming it’s already read into `df`)
# If it's not yet read, use the following (with multiline option):
# df = spark.read.option("multiline", "true").json("/mnt/bronze/bronze/Date=2021-01-01/daily_sync.json")

# Rename and cast columns to match the previous schema
df_transformed = df \
    .withColumnRenamed('invoice_line_no', 'Invoice/Item Number') \
    .withColumnRenamed('date', 'Date') \
    .withColumn('Date', to_date(col('Date'), 'yyyy-MM-dd\'T\'HH:mm:ss.SSS')) \
    .withColumnRenamed('store', 'Store Number') \
    .withColumnRenamed('name', 'Store Name') \
    .withColumnRenamed('address', 'Address') \
    .withColumnRenamed('city', 'City') \
    .withColumnRenamed('zipcode', 'Zip Code') \
    .withColumnRenamed('county_number', 'County Number') \
    .withColumnRenamed('county', 'County') \
    .withColumnRenamed('category', 'Category') \
    .withColumnRenamed('category_name', 'Category Name') \
    .withColumnRenamed('vendor_no', 'Vendor Number') \
    .withColumnRenamed('vendor_name', 'Vendor Name') \
    .withColumnRenamed('itemno', 'Item Number') \
    .withColumnRenamed('im_desc', 'Item Description') \
    .withColumnRenamed('pack', 'Pack') \
    .withColumnRenamed('bottle_volume_ml', 'Bottle Volume (ml)') \
    .withColumnRenamed('state_bottle_cost', 'State Bottle Cost') \
    .withColumnRenamed('state_bottle_retail', 'State Bottle Retail') \
    .withColumnRenamed('sale_bottles', 'Bottles Sold') \
    .withColumnRenamed('sale_dollars', 'Sale (Dollars)') \
    .withColumnRenamed('sale_liters', 'Volume Sold (Liters)') \
    .withColumnRenamed('sale_gallons', 'Volume Sold (Gallons)') \
    .withColumn('Store Number', col('Store Number').cast(IntegerType())) \
    .withColumn('County Number', col('County Number').cast(IntegerType())) \
    .withColumn('Category', col('Category').cast(IntegerType())) \
    .withColumn('Vendor Number', col('Vendor Number').cast(IntegerType())) \
    .withColumn('Pack', col('Pack').cast(IntegerType())) \
    .withColumn('Bottle Volume (ml)', col('Bottle Volume (ml)').cast(IntegerType())) \
    .withColumn('State Bottle Cost', col('State Bottle Cost').cast(DoubleType())) \
    .withColumn('State Bottle Retail', col('State Bottle Retail').cast(DoubleType())) \
    .withColumn('Bottles Sold', col('Bottles Sold').cast(IntegerType())) \
    .withColumn('Sale (Dollars)', col('Sale (Dollars)').cast(DoubleType())) \
    .withColumn('Volume Sold (Liters)', col('Volume Sold (Liters)').cast(DoubleType())) \
    .withColumn('Volume Sold (Gallons)', col('Volume Sold (Gallons)').cast(DoubleType())) \
    .drop('store_location')

# Show the final DataFrame
# df_transformed.show(5)


# COMMAND ----------

# Check for rows where the Date column is null or improperly formatted
df_transformed.filter(col('Date').isNull()).show()


# COMMAND ----------

# Write the data to Parquet format, partitioned by Date
output_path = "/mnt/history/bronze"
df_transformed.write.partitionBy("Date").mode("overwrite").parquet(output_path)

# Verify that files are written
dbutils.fs.ls(output_path)
