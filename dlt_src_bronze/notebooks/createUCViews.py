# Databricks notebook source
#####################################
# Create views in UC on Hive Tables #
#####################################

from pyspark.sql import functions as F
import time

uc_name = "vk_cat"
db_name = "dlt_bronze_config_test"
config_table = "config_sourcefilemetadata"
prefix = "bronze_"

# spark.sql("use catalog hive_metastore")

i = 0

while True:

    df_config = (
        spark.table(db_name + "." + config_table)
        .select(F.col("bronze_table_name"))
        .filter(~F.col("bronze_table_name").like("config%"))
        .orderBy("bronze_table_name")
    )
    tab_names_config = list(prefix + df_config.toPandas()["bronze_table_name"])

    df = (
        spark.sql("show tables in " + db_name)
        .select(F.col("tableName"))
        .filter(~F.col("tableName").like("config%"))
        .orderBy("tableName")
    )
    tab_names = list(df.toPandas()["tableName"])

    if tab_names != tab_names_config:
        # print(" No tables created yet : " + str(i) + " sec elapsed")
        # time.sleep(300)
        # i += 300

        print(" No tables created. Please check DLT task result")
        break

    else:

        # time.sleep(180)

        for tab_name in tab_names:

            df = (
                spark.sql("describe extended " + db_name + "." + tab_name)
                .filter("col_name == 'Location'")
                .select(F.col("data_type"))
            )

            df1 = df.select("*")

            loc = df1.first()[0]

            # spark.sql("use catalog " + uc_name)
            spark.sql(
                "create or replace view "
                + uc_name
                + "."
                + db_name
                + ".vw_"
                + tab_name
                + " as select * from delta.`"
                + loc
                + "`"
            )
            # spark.sql("use catalog hive_metastore")
            print(uc_name + "." + db_name + ".vw_" + tab_name + " on " + loc + " is created in UC ")

        break
