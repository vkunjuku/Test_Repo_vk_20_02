# Databricks notebook source.
# DBTITLE 1,Imports
from pyspark.sql.functions import *
from pyspark.sql.types import *
from itertools import chain
import uuid
import json
from datetime import datetime
from pyspark.sql.functions import udf
from delta.tables import *
from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1,UDF - UUID
uuid_udf = udf(lambda: str(uuid.uuid4()), StringType())

# COMMAND ----------

# DBTITLE 1,Variables
silver_loc = "s3://databricks-raw-bucket/jn/test/silver/"
tbl_prefix = "dlt_silver.silver_"

# COMMAND ----------

# DBTITLE 1,Create Table watermark table
create_watermark_tbl = ' create table if not exists dlt_bronze_cfg.config_watermark (last_processed_date timestamp ,table_name STRING)  USING DELTA LOCATION "s3://databricks-raw-bucket/jn/test/config/config_watermark/" PARTITIONED BY (table_name) '
spark.sql(create_watermark_tbl)

# COMMAND ----------


def create_silver_table(df, table_name, path):
    df.write.format("delta").mode("overwrite").save(path)
    create_tbl = f"""CREATE TABLE IF NOT EXISTS {table_name} USING DELTA LOCATION \"{path}\""""
    spark.sql(create_tbl)
    insert_silver_watermark_stmt = (
        f"""insert into dlt_bronze_cfg.config_watermark values (current_timestamp(),'{table_name}')"""
    )
    spark.sql(insert_silver_watermark_stmt)
    return


# COMMAND ----------


def create_silver_table2(df, table_name, path, partition_cols):
    df.write.format("delta").mode("overwrite").save(path)
    create_tbl = (
        f"""CREATE TABLE IF NOT EXISTS {table_name} USING DELTA LOCATION \"{path}\" PARTITIONED BY ({partition_cols})"""
    )
    spark.sql(create_tbl)
    insert_silver_watermark_stmt = (
        f"""insert into dlt_bronze_cfg.config_watermark values (current_timestamp(),'{table_name}')"""
    )
    spark.sql(insert_silver_watermark_stmt)
    return


# COMMAND ----------


def get_last_processed_date(table_name):
    df_lastprocessed = (
        spark.table("dlt_bronze_cfg.config_watermark")
        .where(f"table_name =='{table_name}'")
        .select("last_processed_date")
    )
    if df_lastprocessed.rdd.isEmpty():
        last_processed_dt = "1900-01-01 00:00:00"
    else:
        last_processed_dt = df_lastprocessed.first()[0]
    return last_processed_dt


# COMMAND ----------


def get_last_read_date():
    df_lastreaddate = spark.sql("select max(meterLastReadDate) as lastreaddate from dlt_bronze.silver_meterreading_dim")
    if df_lastreaddate.rdd.isEmpty():
        last_read_dt = "1900-01-01 00:00:00"
    else:
        last_read_dt = df_lastreaddate.first()[0]
    return last_read_dt


# COMMAND ----------


def update_watermark():
    update_silver_watermark_stmt = f"""update dlt_bronze_cfg.config_watermark set last_processed_date = current_timestamp() where table_name= '{table_name}'"""
    spark.sql(update_silver_watermark_stmt)
    return


# COMMAND ----------

# DBTITLE 1,Schema Functions
def getSparkDataType(colType):
    dataTypeDict = {
        "CHAR": "StringType",
        "NUMBER": "DecimalType",
        "DATE": "TimestampType",
        "VARCHAR2": "StringType",
        "LONG": "LongType",
        "FLOAT": "FloatType",
        "UROWID": "StringType",
        "CLOB": "StringType",
    }
    dataTypeMap = create_map(*[lit(x) for x in chain(*dataTypeDict.items())])
    return dataTypeMap[colType]


def getBronzeTableSchema(sourcename, tableschema, tablename):
    schema = StructType()
    df = spark.table("dlt_bronze_cfg.config_bronzeschema").filter(
        (col("source_system_name") == sourcename)
        & (col("bronze_table_schema") == tableschema)
        & (col("bronze_table_name") == tablename)
    )

    df = df.fillna(0)
    schema = StructType(
        [
            StructField(
                x["bronze_column_name"],
                eval(x["bronze_data_type"] + "()")
                if x["bronze_data_type"] != "DecimalType"
                else eval(x["bronze_data_type"] + "(" + str(x["data_precision"]) + "," + str(x["data_scale"]) + ")"),
                True if x["nulls_allowed"] == "true" else False,
            )
            for x in df.rdd.collect()
        ]
    )
    schema = schema.add("op", StringType(), True)

    return schema


# COMMAND ----------

# DBTITLE 1,Date Related Functions
# from pyspark.sql.functions import udf
# import pyspark.sql.types as t
# from pyspark import SparkContext

# from datetime import datetime
# from array import array
# sc = SparkContext.getOrCreate()

# def getMonthEndDatesInATimePeriod(startdate,enddate,spark):
#   startDt= startdate
#   endDt = enddate
#   print(startDt)
#   print(endDt)

#   df =(spark.table("dlt_bronze.dateDim")\
#        .where(f"dateSID between {startDt} AND {endDt}")\
#        .filter(col("lastdayofmonthind")=='Y')\
#        .select("dateSID"))
# #   display(df)

#   dates = [datetime.strptime(str(x[0]),'%Y%m%d') for x in df.rdd.collect()]
#   endDt= datetime.strptime(str(endDt),'%Y%m%d')
#   if(endDt not in dates):
#     dates.append(endDt)

#   return dates

# getdates = udf(getMonthEndDatesInATimePeriod, t.ArrayType(t.DateType()))
# data = [("1",20190701,20191011),("2",20200624,20200731),("3",20220824,20221116)]
# df=spark.createDataFrame(data=data,schema=["id","prevdate","enddate"])
# df = df.withColumn("dates",getdates('prevdate','enddate',sc))
# display(df)

# COMMAND ----------

# df = getMonthEndDatesInATimePeriod(20220716,20221031)
# # df = df.filter("dateSID=='20220716'")
# print(df)
