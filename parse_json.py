import pandas as pd
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.functions import col, udf
from google.cloud import storage
from datetime import datetime
import json

spark = SparkSession.builder\
                    .master("local[*]") \
                    .appName('app') \
                    .config('spark.jars.packages', 'com.google.cloud.bigdataoss:gcs-connector:hadoop2-1.9.17')\
                    .config('spark.jars.excludes',  'javax.jms:jms,com.sun.jdmk:jmxtools,com.sun.jmx:jmxri')\
                    .config('spark.driver.userClassPathFirst','true')\
                    .config('spark.executor.userClassPathFirst','true')\
                    .config('spark.hadoop.fs.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem')\
                    .config('spark.hadoop.fs.gs.auth.service.account.enable', 'true')\
                    .getOrCreate()

def parse_json(line:str):
    # convert RowType to dict
    line = line.asDict()
    # get value from dict
    record_str = line['value']
    # convert dict value to dictionary
    record = json.loads(record_str)
    if None not in record:
    # [logic to parse records]
        if record['event_type'] == "T":
            # [Get the applicable field values from json]
            return (record["trade_dt"], record["file_tm"], record['event_type'], record["symbol"], record["event_tm"], record["event_seq_nb"], record["exchange"], record["price"], record["size"], None, None, None, None, "T")
        elif record['event_type'] == "Q":
        # [Get the applicable field values from json]
            return (record["trade_dt"], record["file_tm"], record['event_type'], record["symbol"], record["event_tm"], record["event_seq_nb"], record["exchange"], None, None, record['bid_pr'], record['bid_size'],record['ask_pr'], record['ask_size'], "Q")
    else :
        return (None,None,None,None,None,None,None,None,None,None,None,None, None, "B")

print("create initial dataframe") 
df_jsonfile = spark.read.text("gs://equity_market_raw_data_mp/input_data/json/2020-08-06/NASDAQ/part-00000-092ec1db-39ab-4079-9580-f7c7b516a283-c000.txt")
print(type(df_jsonfile))

print("create rdd")
rdd_data = df_jsonfile.rdd

def f(x): print(x)

print("parsed data")
parsed = rdd_data.map(lambda line: parse_json(line))

print("convert rdd to spark df")
df_data = spark.createDataFrame(parsed, ['trade_dt','arrival_tm', 'rec_type','symbol', 'event_tm','event_seq_nb', \
                                         'exchange','trade_pr','trade_size', 'bid_pr', 'bid_size','ask_pr', 'ask_size', 'partition'])
df_data = df_data \
          .withColumn('trade_dt', df_data['trade_dt'].cast(DateType()))\
          .withColumn('arrival_tm', df_data['arrival_tm'].cast(TimestampType()))\
          .withColumn('rec_type', df_data['rec_type'].cast(StringType()))\
          .withColumn('symbol', df_data['symbol'].cast(StringType()))\
          .withColumn('event_tm', df_data['event_tm'].cast(TimestampType()))\
          .withColumn('event_seq_nb', df_data['event_seq_nb'].cast(IntegerType()))\
          .withColumn('exchange', df_data['exchange'].cast(StringType()))\
          .withColumn('trade_pr', df_data['trade_pr'].cast(FloatType()))\
          .withColumn('trade_size', df_data['trade_size'].cast(IntegerType()))\
          .withColumn('bid_pr', df_data['bid_pr'].cast(IntegerType()))\
          .withColumn('bid_size', df_data['bid_size'].cast(FloatType()))\
          .withColumn('ask_pr', df_data['ask_pr'].cast(FloatType()))\
          .withColumn('ask_size', df_data['ask_size'].cast(IntegerType()))\
          .withColumn('partition', df_data['partition'].cast(StringType()))\

print("final df")
df_data.show()
df_data.write.partitionBy("partition").mode("overwrite").parquet("gs://equity_market_raw_data_mp/accepted_json")
print("data written to gcs")