from delta.tables import *
import uuid
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import input_file_name
import json
from datetime import datetime

def apply_silver():
        stg_silver_df=spark.table("dlt_bronze_config_test.stg_cust_add_silver")


        #Reading the target table:
        ins_cust_add_silver=DeltaTable.forPath(spark,"s3://databricks-raw-bucket/code_pipeline_test/test/bronze/tables/tables/cust_add_silver")
        targetDF=ins_cust_add_silver.toDF()

        #Joining the source and target DF 
        joinDf=stg_silver_df.join(targetDF,(stg_silver_df.cust_add_id==targetDF.cust_add_id)&\
                     (targetDF.indicator=="Y"),"leftouter")\
             .select(stg_silver_df["*"],\
                    targetDF.cust_add_sk.alias("tgt_cust_add_sk"),\
                    targetDF.cust_add_id.alias("tgt_cust_add_id"),\
                    targetDF.cust_first_name.alias("tgt_cust_first_name"),\
                    targetDF.cust_last_name.alias("tgt_cust_last_name")
                    )
        #To check the source and target columns that has change:
        # to filter out the records that has no change 
        filterDF=joinDf.filter(xxhash64(joinDf.cust_first_name,joinDf.cust_last_name)
        !=xxhash64(joinDf.tgt_cust_first_name,joinDf.tgt_cust_last_name))

        mergeDf=filterDF.withColumn("MERGEKEY",filterDF.cust_add_id)

        #To filter the non brand new records.For brand new records tgt_cust_id will be null
        dummyDf=filterDF.filter("tgt_cust_add_id is not null").withColumn("MERGEKEY",lit(None))
        sourceDf=mergeDf.union(dummyDf)

        ins_cust_add_silver.alias("tgt").merge(
        source = sourceDf.alias("src"),
        condition = "src.MERGEKEY= tgt.cust_add_id and tgt.indicator='Y'"
        ).whenMatchedUpdate(set =
        {
        "indicator":lit("N"),
        "end_date": "current_date"
        }
        ).whenNotMatchedInsert(values =
        {
        "cust_add_id":"src.cust_add_id",
        "cust_add_sk":"src.cust_add_sk",
        "cust_first_name":"src.cust_first_name",
        "cust_last_name":"src.cust_last_name",
        "ingestdate":"src.ingestdate",
        "ingesttime":"src.ingesttime",
        "indicator":lit("Y"),
        "start_date":"current_date",
        "end_date":"9999-12-31"
        }
        ).execute()
        
save_path="s3://databricks-raw-bucket/code_pipeline_test/test/bronze/tables/tables/cust_add_silver"
if DeltaTable.isDeltaTable(spark, save_path):

#     #Reading the target table:
#     ins_cust_add_silver=DeltaTable.forPath(spark,"s3://databricks-raw-bucket/code_pipeline_test/test/bronze/tables/tables/cust_add_silver")
#     targetDF=ins_cust_add_silver.toDF()

    apply_silver()