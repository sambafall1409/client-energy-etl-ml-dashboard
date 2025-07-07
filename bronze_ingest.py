# Databricks notebook source
# 🔁 Lire les données du layer Bronze (fichier CSV)
df_bronze = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("/mnt/bronzeclientwind/wind_power_production.csv")
#cest modfiee cetta partie cets bohn 

# COMMAND ----------

#testfall
###### load to silver and making trasnformation
df_bronze.write \
    .mode("overwrite") \
    .parquet("/mnt/silverclientwind/wind_power_production_cleaned")
