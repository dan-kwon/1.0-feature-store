# Databricks notebook source
# MAGIC %md
# MAGIC ## DS first-n-day features
# MAGIC A variety of helpful aggregated tables that hold data and features used in various data science models

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load necessary UDFs and libraries

# COMMAND ----------

# MAGIC %run "./_udf__snapshot-features"

# COMMAND ----------

# libraries
import numpy as np
import pandas as pd
from datetime import datetime
from pyspark.sql import functions as F
from dateutil.relativedelta import relativedelta
# global variables
today = datetime.today()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Set date

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.text(name="snapshot_date", 
                     defaultValue=str(today.date()), 
                     label="Snapshot Date")

dbutils.widgets.text(name="horizon_weeks",
                     defaultValue=str(52), 
                     label="Horizon Weeks")

dbutils.widgets.text(name="event_feature_table_name", 
                     defaultValue="ds_staging.features__events_snapshot", 
                     label="Destination Event Feature Table Name")

dbutils.widgets.text(name="engagement_feature_table_name", 
                     defaultValue="ds_staging.features__engagement_snapshot", 
                     label="Destination Engagement Feature Table Name")

# COMMAND ----------

SNAPSHOT_DATE         = dbutils.widgets.get("snapshot_date")
HORIZON_WEEKS         = int(dbutils.widgets.get("horizon_weeks"))
EVENT_TABLE_NAME      = dbutils.widgets.get("event_feature_table_name")
ENGAGEMENT_TABLE_NAME = dbutils.widgets.get("engagement_feature_table_name")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create engagement features

# COMMAND ----------

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

for i in [SNAPSHOT_DATE]: #[pd.to_datetime('2022-06-24').date() - timedelta(days=x) for x in range(4)]
  snapshot_date = str(i)
  df1 = get_engagement(HORIZON_WEEKS, snapshot_date)
  df1 = df1 \
    .groupBy("hs_user_id","snapshot_date") \
    .pivot("weeks_before_snapshot") \
    .agg(*[F.sum(x).alias(x) for x in df1.columns if x not in {"hs_user_id",
                                                               "snapshot_date",
                                                               "weeks_before_snapshot"}])
  
  df2 = get_longest_streak(52, snapshot_date) 
  df3 = get_days_since_last_playback(52, snapshot_date) 
  df4 = df1 \
    .join(df2, how='left', on=['hs_user_id','snapshot_date']) \
    .join(df3, how='left', on=['hs_user_id','snapshot_date'])
  df4 \
    .write \
    .mode('append') \
    .saveAsTable(ENGAGEMENT_TABLE_NAME)
    #.option("mergeSchema", "true") \


# COMMAND ----------

# MAGIC %md
# MAGIC #### Create event features

# COMMAND ----------

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

for i in [SNAPSHOT_DATE]: #[pd.to_datetime('2022-06-24').date() - timedelta(days=x) for x in range(4)]
  snapshot_date = str(i)
  df1 = get_amplitude_events(HORIZON_WEEKS, snapshot_date)
  df1 = df1 \
    .groupBy("hs_user_id","snapshot_date") \
    .pivot("weeks_before_snapshot") \
    .agg(*[F.sum(x).alias(x) for x in df1.columns if x not in {"hs_user_id",
                                                               "snapshot_date",
                                                               "weeks_before_snapshot"}])
  
  df2 = get_braze_events(HORIZON_WEEKS, snapshot_date) 
  df2 = df2 \
    .groupBy("hs_user_id","snapshot_date") \
    .pivot("weeks_before_snapshot") \
    .agg(*[F.sum(x).alias(x) for x in df2.columns if x not in {"hs_user_id",
                                                               "snapshot_date",
                                                               "weeks_before_snapshot"}])
  
  df3 = df1.join(df2, how='left', on=['hs_user_id','snapshot_date'])
  df3 \
    .write \
    .mode('append') \
    .saveAsTable(EVENT_TABLE_NAME)
    #.option("mergeSchema", "true") \
