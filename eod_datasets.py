import pandas as pd
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
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
# read parquet files for trade data from gcs
trade_csv = spark.read.parquet("gs://equity_market_raw_data_mp/accepted_csv/partition=T/part-00000-410d3092-dffb-47b2-8515-e151d13d3d27.c000.snappy.parquet")
trade_json = spark.read.parquet("gs://equity_market_raw_data_mp/accepted_json/partition=T/part-00000-24029700-55e0-422f-811e-76b8d8ef2bdc.c000.snappy.parquet")

# read parquet files for quote data from gcs
quote_csv = spark.read.parquet("gs://equity_market_raw_data_mp/accepted_csv/partition=Q/part-00000-410d3092-dffb-47b2-8515-e151d13d3d27.c000.snappy.parquet")
quote_json = spark.read.parquet("gs://equity_market_raw_data_mp/accepted_json/partition=Q/part-00000-24029700-55e0-422f-811e-76b8d8ef2bdc.c000.snappy.parquet")

# union both dataframes
df_trade = trade_csv.union(trade_json)
df_quote = quote_csv.union(quote_json)

# select relevant columns from both trade and quote dataframes
df_trade = df_trade.select("trade_dt", "rec_type","symbol", "exchange", "event_tm",
"event_seq_nb", "arrival_tm", "trade_pr", "trade_size")

df_quote = df_quote.select("trade_dt", "rec_type","symbol", "exchange", "event_tm",
"event_seq_nb", "arrival_tm", "bid_pr", "bid_size", "ask_pr", "ask_size")

def applyLatest(df):
    # group rows by unique identifier and order by arrival time desc
    win = Window.partitionBy("trade_dt", "symbol", "exchange", "event_tm", "event_seq_nb").orderBy(desc("arrival_tm"))
    df = df.withColumn("rank", rank().over(win))
    df = df.filter(df.rank == 1).drop("rank")
    return df

# retain only the most recent record
df_trade_corrected = applyLatest(df_trade)
df_quote_corrected = applyLatest(df_quote)

# write transformed datasets back to cloud storage
df_trade_corrected.write.parquet("gs://equity_market_raw_data_mp/eod/trade_data")
df_quote_corrected.write.parquet("gs://equity_market_raw_data_mp/eod/quote_data")