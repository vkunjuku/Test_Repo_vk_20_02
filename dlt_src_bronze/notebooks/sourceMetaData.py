# Databricks notebook source
# MAGIC %run ./util_functions

# COMMAND ----------

df= spark.read.load("s3://databricks-raw-bucket/code_pipeline_test/peace_ex_schema/peace_ex_schema.json",format="json",multiLine=True)
#df = spark.read.load(
#   "file:///Workspace/Repos/jason.nam@versent.com.au/dlt_bronze/dlt_src_bronze/config/peace_ex_schema.json",
#   format="json",
#   multiLine=True,
#)

# COMMAND ----------

# DBTITLE 1,Create Source File Metadata
table_df = (
    df.select(explode("tables").alias("tables"))
    .select("*", "tables.*")
    .drop("tables")
    .withColumn("source_system_name", lit("peace_ex"))
    .withColumn("raw_table_schema", lower(col("table_schema")))
    .withColumn("raw_table_name", lower(col("table_name")))
    .withColumn("bronze_table_schema", concat(col("source_system_name"), col("raw_table_schema")))
    .withColumn("bronze_table_name", col("raw_table_name"))
    .withColumn(
        "landing_path",
        concat(
            lit("s3://databricks-raw-bucket/code_pipeline_test/peace_ex_schema/"),
            col("raw_table_schema"),
            lit("/"),
            col("raw_table_name"),
            lit("/"),
        ),
    )
    .withColumn("cron_frequency", lit("* * * *"))
    .withColumn("file_format", lit("parquet"))
    .withColumn("is_active", lit("Y"))
    .withColumn("partition_key", col("partition_keys"))
    .withColumn("partition_by", expr("transform(partition_key, x -> lower(x))"))
    .select(
        "source_system_name",
        "raw_table_schema",
        "raw_table_name",
        "bronze_table_schema",
        "bronze_table_name",
        "landing_path",
        "cron_frequency",
        "file_format",
        "is_active",
        "partition_by",
    )
)

table_df.write.mode("overwrite").option("mergeSchema", True).option(
    "path", "s3://databricks-raw-bucket/code_pipeline_test/config/config_sourcefilemetadata/"
).saveAsTable("dlt_bronze_config_test.config_sourcefilemetadata")

# COMMAND ----------

# DBTITLE 1,Create Bronze Schema
df= spark.read.load("s3://databricks-raw-bucket/code_pipeline_test/peace_ex_schema/peace_ex_schema.json",format="json",multiLine=True)
#df = spark.read.load(
#    "file:///Workspace/Repos/jason.nam@versent.com.au/dlt_bronze/dlt_src_bronze/config/peace_ex_schema.json",
#    format="json",
#   multiLine=True,
#)
column_df = (
    df.select(explode("tables").alias("tables"))
    .select("*", "tables.*")
    .drop("tables")
    .withColumn("source_system_name", lit("peace_ex"))
    .withColumn("raw_table_schema", lower(col("table_schema")))
    .withColumn("raw_table_name", lower(col("table_name")))
    .withColumn("bronze_table_schema", concat(col("source_system_name"), col("raw_table_schema")))
    .withColumn("bronze_table_name", col("raw_table_name"))
    .select(
        "source_system_name",
        "raw_table_schema",
        "raw_table_name",
        "bronze_table_schema",
        "bronze_table_name",
        "primary_keys",
        explode("columns").alias("columns"),
    )
    .select("*", "columns.*")
    .withColumn("raw_column_name", lower(col("column_name")))
    .withColumn("bronze_column_name", lower(col("column_name")))
    .withColumn(
        "is_primary_key", when(array_contains(col("primary_keys"), col("column_name")), "True").otherwise("False")
    )
    .withColumn("bronze_data_type", getSparkDataType(col("data_type")))
    .drop("columns", "primary_keys", "column_name")
    .withColumnRenamed("data_type", "raw_data_type")
)
column_df.write.mode("overwrite").option("mergeSchema", True).option(
    "path", "s3://databricks-raw-bucket/code_pipeline_test/config/config_bronzeschema/"
).saveAsTable("dlt_bronze_config_test.config_bronzeschema")
