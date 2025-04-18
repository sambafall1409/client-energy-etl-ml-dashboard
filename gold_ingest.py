# Databricks notebook source

#load adata silver 
df_silver = spark.read.parquet("/mnt/silverclientwind/wind_power_production_enriched")
#dime dates
display(df_silver)

# COMMAND ----------

#dim time 
dim_time=(df_silver
.select("time","hour","minute","second").distinct().withColumnRenamed("time","time_id")
)
display(dim_time)


# COMMAND ----------

dim_date=(df_silver
.select("date","year","month","day","quarter").distinct()
.withColumnRenamed("date","date_id")

)
display(dim_date)

# COMMAND ----------

#dim ostatus operation 
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
windowSpec = Window.orderBy("status", "responsible_department")

dim_status = (
    df_silver
    .select("status", "responsible_department")
    .distinct()
    .withColumn("status_id", row_number().over(windowSpec))
)
display(dim_status)

# COMMAND ----------


from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
windowSpec = Window.orderBy("turbine_name", "capacity","latitude","longitude","region")
dim_turbine = (
    df_silver
    .select("turbine_name", "capacity","latitude","longitude","region")
    .distinct()
    .withColumn("turbine_id", row_number().over(windowSpec))
)
display(dim_turbine)

# COMMAND ----------

# Jointures avec les dimensions
fact = (
    df_silver
    # Join avec dim_date
    .join(dim_date, df_silver["date"] == dim_date["date_id"], "left")
    # Join avec dim_time
    .join(dim_time, df_silver["time"] == dim_time["time_id"], "left")
    # Join avec dim_turbine
    .join(dim_turbine, ["turbine_name", "capacity", "latitude", "longitude", "region"], "left")
    # Join avec dim_status
    .join(dim_status, ["status", "responsible_department"], "left")
    .select(
        "production_id",
        "date_id",
        "time_id",
        "turbine_id",
        "status_id",
        "wind_speed",
        "wind_direction",
        "energy_produced"
    )
)
display(fact)