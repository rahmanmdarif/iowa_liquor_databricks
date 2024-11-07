# Databricks notebook source
# MAGIC %md
# MAGIC ### Mount

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read the Iowa Liquor Sales data parquet file 

# COMMAND ----------

from pyspark.sql.functions import col

# Define the date range you want to filter
start_date = "2021-01-01"
end_date = "2021-12-31"

# Read only the partitions that fall within the specified date range
df = spark.read.parquet("/mnt/history/").filter(
    (col("Date") >= start_date) & (col("Date") <= end_date)
)

# df = spark.read.parquet("/mnt/history/")

# Proceed with the transformation
df.show()


# COMMAND ----------

# Proceed with the transformation
df.show()

# COMMAND ----------

# Show the schema of the loaded data
df.printSchema()

# Show the first 5 records
df.show(5)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Category

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql.functions import col, row_number, max, to_date, coalesce

# Step 2: Cast Category to integer, perform the GROUP BY and aggregation
df_category = df.where(col("Category").isNotNull()).groupBy(
    col("Category").cast("int").alias("ID_CATEGORY"), 
    col("Category Name")
).agg(
    max(to_date(col("Date"), "yyyy-MM-dd")).alias("Date")  # Adjust format if necessary
)

# Step 3: Apply window function to partition by ID_CATEGORY and order by Date desc
window_spec = Window.partitionBy("ID_CATEGORY").orderBy(col("Date").desc())

df_category_with_rn = df_category.withColumn(
    "rn", row_number().over(window_spec)
)

# Step 4: Filter to get only the latest record for each category (where rn = 1)
df_dim_category = df_category_with_rn.filter(col("rn") == 1).select(
    "ID_CATEGORY", 
    coalesce(col("Category Name"), col("ID_CATEGORY")).alias("Category Name")
)


output_path = "/mnt/silver/category.parquet"
df_dim_category.write.mode("overwrite").parquet(output_path)

# Rename the "Category Name" column to "Category_Name" (or another valid name)
df_dim_category_clean = df_dim_category.withColumnRenamed("Category Name", "Category_Name")

# Now save the DataFrame as a table
df_dim_category_clean.write.mode("overwrite").saveAsTable("default.category")

# COMMAND ----------

df_dim_category.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM default.category;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Calendar

# COMMAND ----------

from pyspark.sql.functions import to_date

# Create the dim_calendar table
df_dim_calendar = df.where(col("Date").isNotNull()).select(
    col("Date").alias("ID_CALENDAR"),
    to_date(col("Date"), 'yyyy-MM-dd').alias("Date")  # Adjusted date format
).distinct()

# Save the dim_calendar table
output_path = "/mnt/silver/calendar.parquet"
df_dim_calendar.write.mode("overwrite").parquet(output_path)
df_dim_calendar.write.mode("overwrite").saveAsTable("default.calendar")


# COMMAND ----------

from pyspark.sql.functions import min

# Get the minimum date from the "Date" column
min_date = df.select(min(col("Date"))).collect()

# COMMAND ----------

df_dim_calendar.write.mode("overwrite").saveAsTable("default.calendar")

# COMMAND ----------

# MAGIC %md
# MAGIC ### County

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql.functions import row_number, max, col, to_date

# Create the dim_county table
df_dim_county = df.where(col("County Number").isNotNull()).groupBy(
    col("County Number").cast("int").alias("ID_COUNTY"), 
    col("County")
).agg(
    max(to_date(col("Date"), "yyyy-MM-dd")).alias("Date")
)

# Apply row_number window function
window_spec = Window.partitionBy("ID_COUNTY").orderBy(col("Date").desc())
df_dim_county_rn = df_dim_county.withColumn("rn", row_number().over(window_spec))

# Filter where rn = 1
df_dim_county_final = df_dim_county_rn.filter(col("rn") == 1).select("ID_COUNTY", "County")

# Save the dim_county table
output_path = "/mnt/silver/county.parquet"
df_dim_county_final.write.mode("overwrite").parquet(output_path)
df_dim_county_final.write.mode("overwrite").saveAsTable("default.county")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Item

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql.functions import col, max, to_date, row_number

# Create the dim_item table
df_dim_item = df.where(col("Item Number").isNotNull()).groupBy(
    col("Item Number").alias("ID_ITEM"), 
    col("Item Description"), 
    col("Pack"), 
    col("Bottle Volume (ml)"), 
    col("State Bottle Cost"), 
    col("State Bottle Retail")
).agg(
    max(to_date(col("Date"), "yyyy-MM-dd")).alias("Date")
)

# Apply row_number window function
window_spec = Window.partitionBy("ID_ITEM").orderBy(col("Date").desc())
df_dim_item_rn = df_dim_item.withColumn("rn", row_number().over(window_spec))

# Filter where rn = 1 and cast columns
df_dim_item_final = df_dim_item_rn.filter(col("rn") == 1).select(
    col("ID_ITEM"),
    col("Item Description"),
    col("Pack").cast("bigint"),
    col("Bottle Volume (ml)").cast("bigint"),
    col("State Bottle Cost").cast("float"),
    col("State Bottle Retail").cast("float"),
    # Calculate Price Per Volume (Lt) Cost
    (col("State Bottle Cost").cast("float") / 
     (col("Bottle Volume (ml)").cast("bigint") * col("Bottle Volume (ml)").cast("bigint") / 1000)
    ).alias("Price Per Volume(Lt) Cost"),
    # Calculate Price Per Volume (Lt) Retail
    (col("State Bottle Retail").cast("float") / 
     (col("Bottle Volume (ml)").cast("bigint") * col("Bottle Volume (ml)").cast("bigint") / 1000)
    ).alias("Price Per Volume(Lt) Retail"),
    # Calculate Price Commission Per Volume (Lt)
    ((col("State Bottle Retail").cast("float") / 
      (col("Bottle Volume (ml)").cast("bigint") * col("Bottle Volume (ml)").cast("bigint") / 1000)) - 
     (col("State Bottle Cost").cast("float") / 
      (col("Bottle Volume (ml)").cast("bigint") * col("Bottle Volume (ml)").cast("bigint") / 1000))
    ).alias("Price Commission Per Volume(Lt)"),
    # Calculate Profit Margin
    ((col("State Bottle Retail").cast("float") / 
      col("State Bottle Cost").cast("float")) - 1
    ).alias("Profit Margin")
)

# Save the dim_item table
output_path = "/mnt/silver/item.parquet"
df_dim_item_final.write.mode("overwrite").parquet(output_path)

# Automatically rename columns with spaces or other invalid characters
for col_name in df_dim_item_final.columns:
    new_col_name = col_name.replace(" ", "_").replace("\n", "").replace("\t", "").replace("(", "").replace(")", "")
    df_dim_item_final = df_dim_item_final.withColumnRenamed(col_name, new_col_name)

# Save the cleaned DataFrame as a table
df_dim_item_final.write.mode("overwrite").option("mergeSchema", "true").saveAsTable("default.item")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Store

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql.functions import row_number, max, col, to_date

# Step 1: Source Data
df_dim_store = df.where(col("Store Number").isNotNull()).groupBy(
    col("Store Number").cast("int").alias("ID_STORE"),
    col("Store Name"),
    col("Address"),
    col("City"),
    col("Zip Code"),
    col("Store Location")
).agg(
    max(to_date(col("Date"), "yyyy-MM-dd")).alias("Date")
)

# Step 2: Apply row_number window function
window_spec = Window.partitionBy("ID_STORE").orderBy(col("Date").desc())
df_dim_store_rn = df_dim_store.withColumn("rn", row_number().over(window_spec))

# Step 3: Filter where rn = 1
df_dim_store_final = df_dim_store_rn.filter(col("rn") == 1).select(
    "ID_STORE", 
    "Store Name", 
    "Address", 
    "City", 
    "Zip Code", 
    "Store Location"
)

# Save the dim_store table
output_path = "/mnt/silver/store.parquet"
df_dim_store_final.write.mode("overwrite").parquet(output_path)

# Automatically rename columns with invalid characters (spaces)
for col_name in df_dim_store_final.columns:
    new_col_name = col_name.replace(" ", "_").replace("\n", "").replace("\t", "")
    df_dim_store_final = df_dim_store_final.withColumnRenamed(col_name, new_col_name)

# Save the cleaned DataFrame as a table
df_dim_store_final.write.mode("overwrite").saveAsTable("default.store")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Vendor

# COMMAND ----------

from pyspark.sql.functions import lpad, col, to_date, max

# Step 1: Cast 'Vendor Number' to string and pad with leading zeros
df = df.withColumn("Vendor_Number_Str", lpad(col("Vendor Number").cast("string"), 4, '0'))

# Step 2: Source Data - Apply the lpad function to pad the Vendor Number to 4 characters
df_dim_vendor = df.where(col("Vendor Number").isNotNull()).groupBy(
    col("Vendor_Number_Str").alias("ID_VENDOR"),  # The padded Vendor Number
    col("Vendor Name")
).agg(
    max(to_date(col("Date"), "yyyy-MM-dd")).alias("Date")
)

# Step 3: Apply row_number window function
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

window_spec = Window.partitionBy("ID_VENDOR").orderBy(col("Date").desc())
df_dim_vendor_rn = df_dim_vendor.withColumn("rn", row_number().over(window_spec))

# Step 4: Filter where rn = 1 and select relevant columns
df_dim_vendor_final = df_dim_vendor_rn.filter(col("rn") == 1).select(
    "ID_VENDOR", 
    "Vendor Name"
)

# Save the dim_vendor table
output_path = "/mnt/silver/vendor.parquet"
df_dim_vendor_final.write.mode("overwrite").parquet(output_path)



# Rename the "Vendor Name" column to "Vendor_Name"
df_dim_vendor_clean = df_dim_vendor_final.withColumnRenamed("Vendor Name", "Vendor_Name")

df_dim_vendor_clean.write.mode("overwrite").saveAsTable("default.vendor")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Sales

# COMMAND ----------

from pyspark.sql.functions import col, lpad, to_date

# Step 1: Pad 'Vendor Number' with leading zeros
df_fact_sales = df.withColumn("Vendor_Number_Str", lpad(col("Vendor Number").cast("string"), 4, '0'))

# Step 2: Select required columns and cast types
df_fact_sales_final = df_fact_sales.select(
    col("Invoice/Item Number"), 
    col("Date").alias("ID_CALENDAR"),
    col("Store Number").cast("int").alias("ID_STORE"),
    col("County Number").cast("int").alias("ID_COUNTY"),
    col("Category").cast("int").alias("ID_CATEGORY"),
    col("Vendor_Number_Str").alias("ID_VENDOR"),  # Padded Vendor ID
    col("Item Number").alias("ID_ITEM"),
    col("Bottles Sold").cast("int"),
    col("Sale (Dollars)").cast("double"),
    col("Volume Sold (Liters)").cast("double"),
    col("Volume Sold (Gallons)").cast("double")
)

# Save the fact_sales table
output_path = "/mnt/silver/sales.parquet"
df_fact_sales.write.mode("overwrite").parquet(output_path)

df_fact_sales_final = df_fact_sales.select(
    col("Invoice/Item Number").alias("Invoice_Item_Number"),  # Renaming the column
    col("Date").alias("ID_CALENDAR"),
    col("Store Number").cast("int").alias("ID_STORE"),
    col("County Number").cast("int").alias("ID_COUNTY"),
    col("Category").cast("int").alias("ID_CATEGORY"),
    col("Vendor_Number_Str").alias("ID_VENDOR"),  # Padded Vendor ID
    col("Item Number").alias("ID_ITEM"),
    col("Bottles Sold").alias("Bottles_Sold"),
    col("Sale (Dollars)").alias("Sale_Dollars"),
    col("Volume Sold (Liters)").alias("Volume_Sold_Liters"),
    col("Volume Sold (Gallons)").alias("Volume_Sold_Gallons")
)

df_fact_sales_final.write.mode("overwrite").saveAsTable("default.sales")
