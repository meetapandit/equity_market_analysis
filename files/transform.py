import pandas as pd
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from google.cloud import storage
from datetime import datetime, timedelta


spark = SparkSession.builder\
                    .master("local[*]") \
                    .appName('app') \
                    .config('spark.jars.packages', 'com.google.cloud.bigdataoss:gcs-connector:hadoop2-1.9.17')\
                    .config('spark.jars.excludes',  'javax.jms:jms,com.sun.jdmk:jmxtools,com.sun.jmx:jmxri')\
                    .config('spark.driver.userClassPathFirst','true')\
                    .config('spark.executor.userClassPathFirst','true')\
                    .config('spark.hadoop.fs.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem')\
                    .config('spark.hadoop.fs.gs.auth.service.account.enable', 'true')\
                    .enableHiveSupport()\
                    .getOrCreate()

# set current date based on dataset
current_dt = '2020-08-06'
# read parquet files for trade data from gcs
print("read from google cloud bucket")
trade_by_dt = spark.read.parquet("gs://equity_market_raw_data_mp/trade/trade_dt={}".format(current_dt))

# filter for current date
print("select required columns from trade dataframe")
df_trade_curr_dt = trade_by_dt.select("date", "rec_type","symbol", "exchange","event_tm", "event_seq_nb", "trade_pr").filter(trade_by_dt.date == '2020-08-06')

# function to get minutes from event_tm timestamp
minutes = lambda x: x * 1440

# define window function to get all records between current and -30 min
win = Window.partitionBy("date", "symbol", "exchange").orderBy(F.col("event_tm").cast('long')).rangeBetween(-minutes(30), 0)

# calculate moving average with the window function
print("calclulate moving avg column")
df_moving_avg = df_trade_curr_dt.withColumn("mov_avg_pr", F.avg("trade_pr").over(win))

# drop table from metastore if exists
print("drop moving_avg table if exists")
spark.sql("DROP TABLE IF EXISTS default.trade_moving_avg")

# write table to hive metastore
df_moving_avg.write.mode("overwrite").saveAsTable("default.trade_moving_avg")

# calculate previous day's last trade price
print("get prev date")
cur_dt = datetime.strptime('2020-08-06', '%Y-%m-%d')
prev_date = datetime.strftime(cur_dt - timedelta(days=1), "%Y-%m-%d")
print(prev_date)
# read from partition to get previous day dataset for trade
print("read data from google cloud for prev date")
trade_prev_dt = spark.read.parquet("gs://equity_market_raw_data_mp/trade/trade_dt={}".format(prev_date))

# window function to get last_trade_pr for prev_day
win_prev = Window.partitionBy("symbol", "exchange").orderBy(desc("event_tm"))

print("calculate last trade price for prev date")
df_last_tr_pr = trade_prev_dt.withColumn("rank_pr", rank().over(win_prev))
df_last_tr_pr = df_last_tr_pr.filter(df_last_tr_pr.rank_pr == 1).drop(df_last_tr_pr.rank_pr)

# drop table from metastore if exists
spark.sql("DROP TABLE IF EXISTS default.tmp_last_trade_pr")

# write table to hive metastore
df_last_tr_pr.write.mode("overwrite").saveAsTable("default.tmp_last_trade_pr")

# union moving_avg dataset to quotes df
df_moving_avg_pr = spark.sql("SELECT * FROM trade_moving_avg")

# add null columns for columns that exist in quotes bit not in trade dataset
df_moving_avg_pr = df_moving_avg_pr.withColumnRenamed("date", "trade_dt")\
                                  .withColumn("bid_pr",lit(None))\
                                  .withColumn("bid_size",lit(None))\
                                  .withColumn("ask_pr",lit(None))\
                                  .withColumn("ask_size",lit(None))\
                                  .select("trade_dt", "rec_type","symbol","event_tm", "event_seq_nb", "exchange", "bid_pr", \
                                          "bid_size", "ask_pr", "ask_size", "trade_pr", "mov_avg_pr")

# read quote dataset from googke cloud storage
quote_df = spark.read.parquet("gs://equity_market_raw_data_mp/quote/")

# add null values for mov_avg_pr and trade_pr columns
quote_df = quote_df.withColumn("mov_avg_pr",lit(None))\
                  .withColumn("trade_pr",lit(None))\
                  .select("trade_dt", "rec_type","symbol","event_tm", "event_seq_nb", "exchange", "bid_pr", "bid_size", "ask_pr", "ask_size", "trade_pr", "mov_avg_pr")

# union quote and moving_avg_pr dataframes
union_df = quote_df.union(df_moving_avg_pr)

# calculate last non-null value
last_val  = Window.partitionBy("symbol", "exchange").orderBy(desc("event_tm"))

quote_union_update = union_df.withColumn("last_trade_pr", last("trade_pr", ignorenulls = True).over(last_val))\
                            .withColumn("last_mov_avg_pr", last("mov_avg_pr", ignorenulls = True).over(last_val))

# filter for quote records with new columns added
quote_union_update = quote_union_update.filter(quote_union_update.rec_type == 'Q')

# read last_trade_pr tabel fpr prev day from hive metastore 
last_trade_pr = spark.sql("SELECT * FROM tmp_last_trade_pr").withColumnRenamed("trade_pr", "close_pr")

# broadcast join smaller last_trade_pr table with updated quotes table
brd_join_condition = [quote_union_update.symbol == last_trade_pr.symbol, quote_union_update.exchange == last_trade_pr.exchange]
quote_with_last_tr_pr = quote_union_update.join(broadcast(last_trade_pr), brd_join_condition, "leftouter")\
                                          .select(quote_union_update.trade_dt, quote_union_update.rec_type,quote_union_update.symbol, \
                                                  quote_union_update.exchange, quote_union_update.event_tm, quote_union_update.event_seq_nb, \
                                                    coalesce(last_trade_pr.close_pr, lit(0.0)).alias("close_pr"), quote_union_update.bid_pr, quote_union_update.bid_size, quote_union_update.ask_pr, \
                                                        quote_union_update.ask_size, quote_union_update.last_trade_pr, quote_union_update.last_mov_avg_pr, \
                                                            (quote_union_update.bid_pr - coalesce(last_trade_pr.close_pr, lit(0.0))).alias("bid_pr_mv"), (quote_union_update.ask_pr - coalesce(last_trade_pr.close_pr,lit(0.0))).alias("ask_pr_mv"))

quote_with_last_tr_pr = quote_with_last_tr_pr.withColumn("partition_date", quote_union_update["trade_dt"])
quote_with_last_tr_pr.write.partitionBy("partition_date").mode("overwrite").parquet("gs://equity_market_raw_data_mp/quote-trade-analytical")

quote_with_last_tr_pr.show()