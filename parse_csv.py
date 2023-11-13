import pandas as pd
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.functions import col, udf
from google.cloud import storage
from datetime import datetime

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

# schema = StructType([
#     StructField("trade_dt", StringType(), True),
#     StructField("arrival_tm", StringType(), True),
#     StructField("rec_type", StringType(), True),
#     StructField("symbol", StringType(), True),
#     StructField("event_tm", StringType(), True),
#     StructField("event_seq_nb", StringType(), True),
#     StructField("exchange", StringType(), True),
#     StructField("trade_pr", StringType(), True),
#     StructField("bid_size", StringType(), True),
#     StructField("ask_pr", StringType(), True),
#     StructField("ask_size", StringType(), True),
#     StructField("partition", StringType(), True)
# ])

def parse_csv(line:str):
    record_type_pos = 2
    record = line.value.split(",")
  
    # [logic to parse records]
    if record[record_type_pos] == "T":
        return (record[0], record[1], record[2], record[3], record[4], record[5], record[6], record[7], record[8], None, None, None, None, "T")
    
    elif record[record_type_pos] == "Q":
        return (record[0], record[1], record[2], record[3], record[4], record[5], record[6], None, None, record[7], record[8], record[9], record[10], "Q")
    else:
        return (None, None, None, None, None, None, None, None, None, None, None,None, None, "B")
   
print("create initial dataframe") 
df_textfile = spark.read.text("gs://equity_market_raw_data_mp/input_data/csv/2020-08-05/NYSE/part-00000-5e4ced0a-66e2-442a-b020-347d0df4df8f-c000.txt")
# df_textfile.na.drop()
print("create rdd")
rdd_data = df_textfile.rdd

def f(x):
    print(x)
rdd_data.foreach(f)

print("parsed data")
parsed = rdd_data.map(lambda line: parse_csv(line))

print("convert rdd to spark df")
df_data = spark.createDataFrame(parsed, ['trade_dt','arrival_tm', 'rec_type','symbol', 'event_tm', 'event_seq_nb', \
                                         'exchange','trade_pr','trade_size', 'bid_pr','bid_size','ask_pr', 'ask_size', 'partition'])

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
          .withColumn('bid_pr', df_data['bid_pr'].cast(FloatType()))\
          .withColumn('bid_size', df_data['bid_size'].cast(IntegerType()))\
          .withColumn('ask_pr', df_data['ask_pr'].cast(FloatType()))\
          .withColumn('ask_size', df_data['ask_size'].cast(IntegerType()))\
          .withColumn('partition', df_data['partition'].cast(StringType()))\

print("final df")
df_data.show()
df_data.write.partitionBy("partition").mode("overwrite").parquet("gs://equity_market_raw_data_mp/accepted_csv")
print("data written to gcs")
