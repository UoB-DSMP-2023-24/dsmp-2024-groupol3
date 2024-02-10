# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC This notebook will show you how to create and query a table or DataFrame that you uploaded to DBFS. [DBFS](https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html) is a Databricks File System that allows you to store data for querying inside of Databricks. This notebook assumes that you have a file already inside of DBFS that you would like to read from.
# MAGIC
# MAGIC This notebook is written in **Python** so the default cell type is Python. However, you can use different languages by using the `%LANGUAGE` syntax. Python, Scala, SQL, and R are all supported.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Raw Table

# COMMAND ----------

from pyspark.sql.functions import col, when

# File location and type
file_location = "/FileStore/tables/fake_transactional_data_24__1_.csv"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "true"
delimiter = ","

# Read CSV file into DataFrame
df_check = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

# Register DataFrame as a temporary view
df_check.createOrReplaceTempView("temp_table")

# Display the DataFrame
display(df_check)


# COMMAND ----------

# MAGIC %md
# MAGIC ### First column check - account

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query to check if all the entries end in .0
# MAGIC SELECT
# MAGIC   from_totally_fake_account
# MAGIC FROM
# MAGIC   temp_table
# MAGIC WHERE
# MAGIC   from_totally_fake_account NOT LIKE '%.0'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Second column check - random generated account

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   DISTINCT
# MAGIC   from_totally_fake_account,
# MAGIC   to_randomly_generated_account
# MAGIC FROM
# MAGIC   temp_table
# MAGIC ORDER BY
# MAGIC   from_totally_fake_account

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Focus on a single account 
# MAGIC SELECT
# MAGIC   from_totally_fake_account,
# MAGIC   to_randomly_generated_account
# MAGIC FROM
# MAGIC   temp_table
# MAGIC WHERE
# MAGIC  from_totally_fake_account = '1000.0'
# MAGIC ORDER BY
# MAGIC   to_randomly_generated_account

# COMMAND ----------

# MAGIC %md
# MAGIC ## Curated Table

# COMMAND ----------

from pyspark.sql.functions import col, when

# File location and type
file_location = "/FileStore/tables/fake_transactional_data_24__1_.csv"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "true"
delimiter = ","

# Read CSV file into DataFrame
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

# Changing the column names
new_column_names = ["account_id", "purchase_amount", "purchase_type", "date_id"]
df = df.toDF(*new_column_names)

# Check if all values in 'account_id' end with '.0' and remove decimal part
df = df.withColumn("account_id_whole", 
                   when(col("account_id").substr(-2, 2) == ".0", 
                        col("account_id").cast("integer")).otherwise(col("account_id")))
df = df.drop("account_id").withColumnRenamed("account_id_whole", "account_id")

# Reorder columns
df = df.select("account_id", "purchase_amount", "purchase_type", "date_id")

# Display the DataFrame
display(df)


# COMMAND ----------

# Create a view or table

temp_table_name = "fake_transactional_data_24__1__csv"

df.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC /* Query the created temp table in a SQL cell */
# MAGIC
# MAGIC select * from `fake_transactional_data_24__1__csv`

# COMMAND ----------

# With this registered as a temp view, it will only be available to this particular notebook. If you'd like other users to be able to query this table, you can also create a table from the DataFrame.
# Once saved, this table will persist across cluster restarts as well as allow various users across different notebooks to query this data.

raw_transactional_data = "fake_transactional_data_24__1__csv"

df.write.format("parquet").saveAsTable(raw_transactional_data)

# COMMAND ----------


