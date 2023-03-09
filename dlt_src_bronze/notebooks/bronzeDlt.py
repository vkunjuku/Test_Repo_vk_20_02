spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")
#%run ./util_functions
from datetime import datetime
from pyspark.sql.functions import *
from pyspark.sql.types import *
from itertools import chain
import pytz

####Cron Expected###
#    * * * *    #
#    | | | |
#    | | | 0 to 6 (0 = Sunday), egs. * * * 4(Every Thrusday), * * * 1-4(Monday to friday), * * * 1,2,3(Monday, Tuesday, Wednesday)
#    | | 1 to 12 (Month value), egs. * * 12 4 (Thurday Decemeber), * * 1-2 *(Jan & feb every day), * * 3,4 * (March & april everyday)
#    | 1 to 31 (Date), egs. * 1 * * (1st of each month), * 10-15 1 *( Jan 10th to 15th), * 11,22 * * (Every month 11th and 22nd)
#    0 to 23 (Hour), egs. 10 * * * (10 o clock eveyday), 9-18 * * 1(Every monday 9AM to 6 PM), 0,12 * * *(Noon and midnight run everyday)
#
# Cron value includes only in 4 parts and seperated by single space
#
# Limitations
#
# Numerical values are only processed not textual information like Jan-Mar or Thursday - Saturday
#
###################


class cron_parser:
    def __init__(self, cron_str):
        self.cron_str = cron_str
        self.const_list = [list(range(0, 24)), list(range(1, 32)), list(range(1, 13)), list(range(0, 7))]
        self.hour = self.const_list[0]
        self.day = self.const_list[1]
        self.month = self.const_list[2]
        self.weekday = self.const_list[3]
        self.ingest_date = 0
        self.parse()

    def parse_range(self, parse_str):
        parse_list = parse_str.split("-")
        parse_list = list(range(int(parse_list[0]), int(parse_list[1]) + 1))
        return parse_list

    def parse_list(self, parse_str):
        parse_list = parse_str.split(",")
        parse_list = [int(x) for x in parse_list]
        return parse_list

    def parse_str(self, parse_str):
        return [int(parse_str)]

    def parse(self):

        cron_list = self.cron_str.split(" ")

        for i in range(len(cron_list)):
            parsed_list = []
            cron_item = cron_list[i]
            print(cron_item)

            ##Parse str to list
            if "-" in cron_item:
                parsed_list = self.parse_range(cron_item)

            elif "," in cron_item:
                parsed_list = self.parse_list(cron_item)

            elif cron_item != "*":
                parsed_list = self.parse_str(cron_item)

            elif len(cron_item) and (cron_item != "*"):
                raise Exception("Cron not parseble {}".format(self.cron_str))

            if cron_item != "*":
                # get hour list
                if (i == 0) and len(set(parsed_list) - set(self.const_list[0])) == 0:
                    self.hour = parsed_list

                # get day list
                elif (i == 1) and len(set(parsed_list) - set(self.const_list[1])) == 0:
                    self.day = parsed_list

                # get month list
                elif (i == 2) and len(set(parsed_list) - set(self.const_list[2])) == 0:
                    self.month = parsed_list

                # get weeday list
                elif (i == 3) and len(set(parsed_list) - set(self.const_list[3])) == 0:
                    self.weekday = parsed_list
                # raise exception
                else:
                    raise Exception("Cron not parseble {}".format(self.cron_str))

        return

    # check if the ingestion is scheduled now
    def is_schedule_now(self):
        now = datetime.now(pytz.timezone("Australia/Sydney"))

        self.ingest_date = int(ingest_timestamp.strftime("%Y%m%d"))

        Is_Schedule = False

        # Is this the hour to schedule ?
        if (int(now.strftime("%w")) in self.weekday) and (now.day in self.day) and (now.month in self.month):

            if now.hour in self.hour:
                Is_Schedule = True

        return Is_Schedule

    # check if the ingestion is scheduled now
    def is_schedule_today(self):
        now = datetime.now(pytz.timezone("Australia/Sydney"))

        Is_Schedule = False

        # Did the previous schedule go well check ingestion logs?
        # If not schedule it now
        if (int(now.strftime("%w")) in self.weekday) and (now.day in self.day) and (now.month in self.month):

            for schedule_hour in self.hour:
                if now.hour > schedule_hour:
                    Is_Schedule = True

        return Is_Schedule


# Check if previous run dint happen
def previous_ingestion_skipped(cp, table_name):

    raw_df = spark.sql(
        "select max(ingest_timestamp) as last_ingesttime from delta_ingestion_logs where table_name = '"
        + table_name
        + "' and ingest_date = "
        + cp.ingest_date
        + ";"
    )
    last_ingestime = raw_df.first()["last_ingesttime"]

    if last_ingestime is None:
        return True

    return False


# Parse cron string
# Check if the schedule is for today and current hour?
#    # Then Start pipeline
#    # Else check if previous hour ingestion has been skipped from ingestion log table (Some maintenance activity or file arrived late)
#              # Then Start pipeline
# If file dint arrive for more than 24 hrs temporarily change the config file and get the ingestion through
def can_pipeline_start(cron_string):
    pipeline_start = False

    cp = cron_parser(cron_string)

    if cp.is_schedule_now():
        pipeline_start = True

    elif cp.is_schedule_today():  # and previous_ingestion_failed(cp.ingest_date)
        pipeline_start = True

    return pipeline_start


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
    df = spark.table("dlt_bronze_config_test.config_bronzeschema").filter(
        (col("source_system_name") == sourcename)
        & (col("bronze_table_schema") == tableschema)
        & (col("bronze_table_name") == tablename)
    )
    df = df.fillna(10, ["data_precision"]).fillna(0, ["data_scale"])

    #   display(df)
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
import dlt
import uuid
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import input_file_name
import json
from datetime import datetime

# Ingestion Values
no_of_raw_records = 0
no_of_clean_rows = 0
ingest_timestamp = datetime.now()
str_timestamp = ingest_timestamp.strftime("%Y-%m-%d %H:%M:%S.%f")
str_timestamp = str_timestamp[:-3]
ingest_date = int(ingest_timestamp.strftime("%Y%m%d"))

# UTILITITY function for handling column name errors, headers cannot contain " /-,;{}()='"
def rename_cols(df):
    for column in df.columns:
        new_column = column.replace(" ", "_")
        df = df.withColumnRenamed(column, new_column)
    return df


# Replace error buckets
def change_error_bucket(s3_landing_path):
    s3_error_bucket = "databricks-raw-bucket/code_pipeline_test/test/error"

    # Find and replace
    i = s3_landing_path.find("//")
    end_i = s3_landing_path[i + 2 :].find("/")
    extract_bucket = s3_landing_path[i + 2 : end_i + i + 2]
    changed_error_path = s3_landing_path.replace(extract_bucket, s3_error_bucket)

    return changed_error_path
#########################
# Main ingestion pipeline#
#########################
def call_pipeline(table_name, path, customschema, fileformat, partitionby):
    if partitionby == None:
        partitionby = ["ingestdate"]
    elif partitionby == []:
        partitionby = ["ingestdate"]
    elif partitionby == " ":
        partitionby = ["ingestdate"]

       # PipeLine Begins
    @dlt.table(name=table_name+"_1", table_properties={"quality": "bronze"}, partition_cols=partitionby)
    def bronze_raw():
        read_df = (
            spark.readStream.format("cloudFiles")
            .schema(customschema)
            .option("cloudFiles.format", "csv")
            #.option("pathGlobFilter", table_name+".*")
            #Added new code below
            .option("delimiter","|")
            #Commented below command
           # .option("badRecordsPath", change_error_bucket(path))
            .load(path)
            .withColumn("op_type", when(input_file_name().like("%LOAD000%"), lit("I")).otherwise(col("op")))
            .withColumn("filename", input_file_name())
            .withColumn("ingestdate", (lit(datetime.now().strftime("%Y-%m-%d"))))
            .withColumn("ingesttime", to_timestamp(lit(str_timestamp)))
            .drop("op")
        )
        (read_df.writeStream
                .format("delta")
                .trigger(once=True)
                .outputMode("append")
                .option("checkpointlocation","s3://databricks-raw-bucket/code_pipeline_test/test/bronze/checkpoints/")
                .start(table_name))
        

        #     df = read_df.withColumn("filename", input_file_name()).withColumn("ingesttime",to_timestamp(lit(str_timestamp)))
        #print(read_df.count
        
        return rename_cols(read_df)
        


########################
# End of single pipeline#
########################
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from delta.tables import *
 
def call_pipeline_silver():
    
 
   
    @dlt.table(name="stg_cust_add_silver", table_properties={"quality": "silver"})
    def silver_raw():
        
        df_bz_cus=dlt.read("customer_1").filter(col("ingestdate")==current_date())
        df_bz_cus=df_bz_cus.withColumn("rowid",row_number().over(Window.partitionBy("cust_id").orderBy(col("ingesttime").desc())) )
        df_bz_cus_filt=df_bz_cus.filter("rowid=1")

        df_bz_cus_add=dlt.read("customer_address_1").filter(col("ingestdate")==current_date())
        df_bz_cus_add=df_bz_cus_add.withColumn("rowid",row_number().over(Window.partitionBy("cust_add_id").orderBy(col("ingestdate").desc())))
        df_bz_cus_add_filt=df_bz_cus_add.filter("rowid=1")
        df_join=df_bz_cus_filt.join(df_bz_cus_add_filt,df_bz_cus_filt.cust_id==df_bz_cus_add_filt.cust_add_id).select(df_bz_cus_add_filt.cust_add_sk,df_bz_cus_add_filt.cust_add_id,df_bz_cus_filt.cust_first_name,df_bz_cus_filt.cust_last_name).withColumn("ingestdate", (lit(datetime.now().strftime("%Y-%m-%d")))).withColumn("ingesttime", to_timestamp(lit(str_timestamp)))
        return df_join
    
    
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

############################
# Read list of files to process#
############################

ingest_df = spark.table("dlt_bronze_config_test.config_sourcefilemetadata").filter("is_active ='Y'")

for row in ingest_df.collect():
    table_name = row["bronze_table_name"]
    table_schema = row["bronze_table_schema"]
    source_name = row["source_system_name"]
    #    expect_constraints = json.loads(row['Expect_all'])
    path = row["landing_path"]
    fileformat = row["file_format"]
    cron_string = row["cron_frequency"]
    partitionby = row["partition_by"]
    schema = getBronzeTableSchema(source_name, table_schema, table_name)
    

    if can_pipeline_start(cron_string):
        
        call_pipeline(table_name, path, schema, fileformat, partitionby)
call_pipeline_silver()

#save_path="s3://databricks-raw-bucket/code_pipeline_test/test/bronze/tables/tables/cust_add_silver"
#if DeltaTable.isDeltaTable(spark, save_path):

#     #Reading the target table:
#     ins_cust_add_silver=DeltaTable.forPath(spark,"s3://databricks-raw-bucket/code_pipeline_test/test/bronze/tables/tables/cust_add_silver")
#     targetDF=ins_cust_add_silver.toDF()

#    apply_silver()   