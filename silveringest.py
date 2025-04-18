# Databricks notebook source
df_silver = spark.read.parquet("/mnt/silverclientwind/wind_power_production_cleaned")
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col, round, dayofmonth, month, year, to_date, to_timestamp,
    quarter, hour, minute, second,
    substring, when, regexp_replace,
    row_number, window
)

df_silver = (
    df_silver
    .withColumn("month", month(col("date")))
    .withColumn("day", dayofmonth(col("date")))
    .withColumn("quarter", quarter(col("date")))
    .withColumn("year", year(col("date")))
    .withColumn("latitude", round(col("latitude")))
    .withColumn("longitude", round(col("longitude")))
    .withColumn("hour", substring(col("time"),1,2))
    .withColumn("minute", substring(col("time"),4,2))
    .withColumn("second", substring(col("time"),7,2))
)


df_silver.write.mode("overwrite").parquet("/mnt/silverclientwind/wind_power_production_enriched")

display(df_silver)




# COMMAND ----------

