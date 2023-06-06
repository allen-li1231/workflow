from pyspark import SparkConf, SparkContext, StorageLevel
from pyspark.sql import SparkSession, Window, Row, Column
from pyspark.sql import functions as F
from pyspark.sql.functions import col, udf, struct
from pyspark.sql.types import StructType, StructField, IntegerType, ArrayType, FloatType, StringType

import numpy as np
import pandas as pd

import datetime as dt
from datetime import date, datetime


conf = (
    SparkConf()
   .setAppName("lizhonghao_industry_org_time_anomaly_analysis")
   .set("livy.spark.sql.execution.arrow.pyspark.enabled", "true")
   .set("livy.spark.sql.inMemoryColumnarStorage.compressed", "true")
   .set("livy.spark.hadoop.hive.input.dir.recursive", "true")
   .set("livy.spark.hadoop.hive.mapred.supports.subdirectories", "true")
   .set("livy.spark.hadoop.hive.supports.subdirectories", "true")
   .set("livy.spark.hadoop.hive.mapred.input.dir.recursive", "true")
#   .set("spark.executor.memory", "4g")
#   .set("spark.executor.instances", "4") # 2
#   .set("spark.executor.cores", "16")
#   .set("spark.task.cpus", "4")
#   .set("spark.yarn.executor.memoryOverhead", "2g")
)

spark = (
    SparkSession
    .builder
    .config(conf=conf)
    .enableHiveSupport()
    .getOrCreate()
)
#%zf_fk_spark.pyspark
spark.sql("show functions").show(500)
#%zf_fk_spark
import os
os.path.abspath(".")
#%zf_fk_spark.pyspark
import sys
sys.path
#%zf_fk_spark.pyspark
sdf_trans = spark.sql("select merchant_id, cast(ymd as int) from buffer_fk.lzh_recent_90days_opo where ymd between 20230501 and 20230503")
#%zf_fk_spark.pyspark
sdf_trans.groupby("merchant_id").count()
#%zf_fk_spark.pyspark
sdf_trans.select("merchant_id").head(10)
#%zf_fk_spark.pyspark
df_mer = sdf_trans.limit(10).toPandas()
df_mer
#%zf_fk_spark.pyspark
sdf_top_cnt_trans_mer = (
    sdf_trans
    .groupby(["merchant_id", "ymd"])
    .count()
    .orderBy(col("count").desc())
    .limit(10)
)
sdf_top_cnt_trans_mer.show()
#%zf_fk_spark.pyspark
(
    
    sdf_top_cnt_trans_mer
    .write
    .format("orc")
    .option("batchsize","100000")
    .mode("overwrite") # overwrite/append/ignore/error
    .saveAsTable("buff_fk.lzh_spark_intro")
)
#%zf_fk_spark.pyspark
# (
    
#     sdf_top_cnt_trans_mer
#     .write
#     .format("orc")
#     .option("batchsize","100000")
#     .mode("overwrite") # overwrite/append/ignore/error
#     .saveAsTable("buff_fk.lzh_spark_intro")
# )
(
    sdf_top_cnt_trans_mer
    .repartition(1)
    .write
    #.option("fetchsize","100000")
    .option("batchsize","100000")
    .mode("overwrite") # overwrite/append/ignore/error
    .csv("/user/lizhonghao/output/sparkIntro.csv")
)
#%zf_fk_spark.pyspark
sdf.schema
#%zf_fk_spark.pyspark
sdf = spark.sql("select merchant_id, lkl_merchant_id from dwf.merchant where lkl_merchant_id = 4002021110927553026.")
df = sdf.toPandas()
df["lkl_merchant_id"] = df["lkl_merchant_id"].astype(int)
df
#%zf_fk_spark.pyspark
int(df["lkl_merchant_id"][0])
#%zf_fk_spark.pyspark

import os