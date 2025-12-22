# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

import os
import sys
project_path = os.path.join(os.getcwd(),'..','..')
sys.path.append(project_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ### **DimUser**

# COMMAND ----------

df = spark.read.format("parquet")\
    .load("abfss://bronze@jayazureproject.dfs.core.windows.net/DimUser")

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT READ FILES ON EXTERNAL LOCATION `bronze` TO `Admins`;

# COMMAND ----------

# MAGIC %md
# MAGIC #### **AUTOLOADER**

# COMMAND ----------

df_user = spark.readStream.format("cloudFiles")\
                .option("cloudFiles.format", "parquet")\
                .option("cloudFiles.schemaLocation", "abfss://silver@jayazureproject.dfs.core.windows.net/DimUser/checkpoint")\
                .load("abfss://bronze@jayazureproject.dfs.core.windows.net/DimUser")

# COMMAND ----------

display(df_user)

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT WRITE FILES ON EXTERNAL LOCATION `silver` TO `Admins`;

# COMMAND ----------

df_user = df_user.withColumn("user_name", upper(col("user_name")))
display(df_user)

# COMMAND ----------

print(os.getcwd())

# COMMAND ----------

from utils.transformations import restart

# COMMAND ----------

df_user_obj = restart()

df_user = df_user_obj.dropColumns(df_user, ['_rescued_data'])
df_user = df_user.dropDuplicates(['user_id'])
display(df_user)

# COMMAND ----------

df_user.writeStream.format("delta")\
    .option("checkpointLocation", "abfss://silver@jayazureproject.dfs.core.windows.net/DimUser/checkpoint")\
    .trigger(once=True)\
    .option("path","abfss://silver@jayazureproject.dfs.core.windows.net/DimUser/data")\
    .toTable("spotify_cata.silver.DimUser")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from spotify_cata.silver.DimUser

# COMMAND ----------

# MAGIC %md
# MAGIC ### **DimArtist**

# COMMAND ----------

df_artist = spark.readStream.format("cloudfiles")\
                .option("cloudFiles.format", "parquet")\
                .option("cloudFiles.schemaLocation", "abfss://silver@jayazureproject.dfs.core.windows.net/DimArtist/checkpoint")\
                .option("schemaEvolution", "addNewColumns")\
                .load("abfss://bronze@jayazureproject.dfs.core.windows.net/DimArtist")
display(df_artist)

# COMMAND ----------

df_artist_obj = restart()

df_artist = df_artist_obj.dropColumns(df_artist, ['_rescued_data'])
df_artist = df_artist.dropDuplicates(['artist_id'])
display(df_artist)


# COMMAND ----------

df_artist.writeStream.format('delta')\
    .option("checkpointLocation", "abfss://silver@jayazureproject.dfs.core.windows.net/DimArtist/checkpoint")\
    .trigger(once=True)\
    .option("path", "abfss://silver@jayazureproject.dfs.core.windows.net/DimArtist/data")\
    .toTable("spotify_cata.silver.DimArtist")

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT USE CATALOG ON CATALOG spotify_cata TO Admins;

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT USE SCHEMA ON SCHEMA spotify_cata.silver TO Admins;
# MAGIC GRANT CREATE TABLE ON SCHEMA spotify_cata.silver TO Admins;

# COMMAND ----------

# MAGIC %md
# MAGIC ### **DimTrack**

# COMMAND ----------

df_track = spark.readStream.format("cloudfiles")\
                .option("cloudFiles.format", "parquet")\
                .option("cloudFiles.schemaLocation", "abfss://silver@jayazureproject.dfs.core.windows.net/DimTrack/checkpoint")\
                .option("schemaEvolution", "addNewColumns")\
                .load("abfss://bronze@jayazureproject.dfs.core.windows.net/DimTrack")

# COMMAND ----------

display(df_track)

# COMMAND ----------

df_track = df_track.withColumn("durationFlag", when(col("duration_sec") < 150, "low")\
                                               .when(col("duration_sec") < 300, "medium")\
                                                .otherwise("high"))

df_track = df_track.withColumn("track_name", regexp_replace(col('track_name'), "-", " "))

df_track = restart().dropColumns(df_track, ["_rescued_data"])

df_track.display()

# COMMAND ----------

df_track.writeStream.format('delta')\
                    .outputMode("append")\
                    .option("checkpointlocation", "abfss://silver@jayazureproject.dfs.core.windows.net/DimTrack/checkpoint")\
                    .trigger(once=True)\
                    .option("path", "abfss://silver@jayazureproject.dfs.core.windows.net/DimTrack/data")\
                    .toTable("spotify_cata.silver.DimTrack")

# COMMAND ----------

# MAGIC %md
# MAGIC ### **DimDate**

# COMMAND ----------

df_date = spark.readStream.format("cloudfiles")\
                .option("cloudFiles.format", "parquet")\
                .option("cloudFiles.schemaLocation", "abfss://silver@jayazureproject.dfs.core.windows.net/DimDate/checkpoint")\
                .option("schemaEvolution", "addNewColumns")\
                .load("abfss://bronze@jayazureproject.dfs.core.windows.net/DimDate")

# COMMAND ----------

df_date = restart().dropColumns(df_date, ["_rescued_data"])

df_date.writeStream.format('delta')\
                    .outputMode("append")\
                    .option("checkpointlocation", "abfss://silver@jayazureproject.dfs.core.windows.net/DimDate/checkpoint")\
                    .trigger(once=True)\
                    .option("path", "abfss://silver@jayazureproject.dfs.core.windows.net/DimDate/data")\
                    .toTable("spotify_cata.silver.DimDate")

# COMMAND ----------

# MAGIC %md
# MAGIC ### **FactStream**

# COMMAND ----------

df_fact = spark.readStream.format("cloudfiles")\
                .option("cloudFiles.format", "parquet")\
                .option("cloudFiles.schemaLocation", "abfss://silver@jayazureproject.dfs.core.windows.net/FactStream/checkpoint")\
                .option("schemaEvolution", "addNewColumns")\
                .load("abfss://bronze@jayazureproject.dfs.core.windows.net/FactStream")

# COMMAND ----------

df_fact = restart().dropColumns(df_fact, ["_rescued_data"])

df_fact.writeStream.format('delta')\
                    .outputMode("append")\
                    .option("checkpointlocation", "abfss://silver@jayazureproject.dfs.core.windows.net/FactStream/checkpoint")\
                    .trigger(once=True)\
                    .option("path", "abfss://silver@jayazureproject.dfs.core.windows.net/FactStream/data")\
                    .toTable("spotify_cata.silver.FactStream")

# COMMAND ----------

# MAGIC %sql
# MAGIC --select * from spotify_cata.gold.dimuser_stg
# MAGIC --select * from spotify_cata.gold.dimuser
# MAGIC
# MAGIC --drop materialized view spotify_cata.gold.dimuser_stg
# MAGIC drop table spotify_cata.gold.dimuser

# COMMAND ----------

