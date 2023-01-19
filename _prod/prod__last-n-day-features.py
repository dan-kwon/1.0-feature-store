# Databricks notebook source
# MAGIC %md
# MAGIC ## DS first-n-day features
# MAGIC A variety of helpful aggregated tables that hold data and features used in various data science models

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load necessary UDFs and libraries

# COMMAND ----------

# MAGIC %run "./prod__helper_library_last-n-days"

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

dbutils.widgets.text(name="specified_date", 
                     defaultValue=str(today.date() - relativedelta(days=1)), 
                     label="Specified Date")

dbutils.widgets.text(name="horizon_days",
                     defaultValue=str(28), 
                     label="Horizon Days")

dbutils.widgets.text(name="event_feature_table_name", 
                     defaultValue="ds_staging.features__events_last_n_days", 
                     label="Destination Event Feature Table Name")

dbutils.widgets.text(name="engagement_feature_table_name", 
                     defaultValue="ds_staging.features__engagement_last_n_days", 
                     label="Destination Engagement Feature Table Name")

# COMMAND ----------

PAID_SUBSCRIPTION_END_DATE = dbutils.widgets.get("specified_date")
HORIZON_DAYS               = int(dbutils.widgets.get("horizon_days"))
EVENT_TABLE_NAME           = dbutils.widgets.get("event_feature_table_name")
ENGAGEMENT_TABLE_NAME      = dbutils.widgets.get("engagement_feature_table_name")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create engagement features

# COMMAND ----------

# new users
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

for i in [PAID_SUBSCRIPTION_END_DATE]: #[pd.to_datetime("2021-12-31").date() - timedelta(days=x) for x in range(365)] 
  paid_subscription_end_date = str(i)
  df1 = get_last_engagement(HORIZON_DAYS, paid_subscription_end_date)
  df1 = df1 \
    .groupBy("hs_user_id","paid_subscription_end_date") \
    .pivot("days_until_cancel") \
    .agg(*[F.sum(x).alias(x) for x in df1.columns if x not in {"hs_user_id",
                                                               "free_trial_start_date",
                                                               "free_trial_end_date",
                                                               "paid_subscription_start_date",
                                                               "paid_subscription_end_date",
                                                               "days_until_cancel"}])
  
  df2 = get_last_distinct_engagement(HORIZON_DAYS-1, paid_subscription_end_date) 
  df2 = df2 \
    .groupBy("hs_user_id","paid_subscription_end_date") \
    .pivot("weeks_until_cancel") \
    .agg(*[F.sum(x).alias(x) for x in df2.columns if x not in {"hs_user_id",
                                                               "weeks_until_cancel",
                                                               "free_trial_start_date",
                                                               "free_trial_end_date",
                                                               "paid_subscription_start_date",
                                                               "paid_subscription_end_date",
                                                               "days_until_cancel"}])
  
  df3 = df1.join(df2, how='left', on=['hs_user_id','paid_subscription_end_date'])
  df3.na.fill(0) \
    .write \
    .mode('append') \
    .option("mergeSchema", "true") \
    .saveAsTable(ENGAGEMENT_TABLE_NAME)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create event features

# COMMAND ----------

# new users
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

for i in [PAID_SUBSCRIPTION_END_DATE]: #[pd.to_datetime("2021-12-31").date() - timedelta(days=x) for x in range(365)] 
  paid_subscription_end_date = str(i)
  df1 = get_last_events(HORIZON_DAYS, paid_subscription_end_date)
  df1 = df1 \
    .groupBy("hs_user_id","paid_subscription_end_date") \
    .pivot("days_until_cancel") \
    .agg(*[F.sum(x).alias(x) for x in df1.columns if x not in {"hs_user_id",
                                                               "free_trial_start_date",
                                                               "free_trial_end_date",
                                                               "paid_subscription_start_date",
                                                               "paid_subscription_end_date",
                                                               "days_until_cancel"}])
  
  df2 = get_last_distinct_events(HORIZON_DAYS-1, paid_subscription_end_date) 
  df2 = df2 \
    .groupBy("hs_user_id","paid_subscription_end_date") \
    .pivot("weeks_until_cancel") \
    .agg(*[F.sum(x).alias(x) for x in df2.columns if x not in {"hs_user_id",
                                                               "weeks_until_cancel",
                                                               "free_trial_start_date",
                                                               "free_trial_end_date",
                                                               "paid_subscription_start_date",
                                                               "paid_subscription_end_date",
                                                               "days_until_cancel"}])
  
  df3 = df1.join(df2, how='left', on=['hs_user_id','paid_subscription_end_date'])
  df3.na.fill(0) \
    .write \
    .mode('append') \
    .option("mergeSchema", "true") \
    .saveAsTable(EVENT_TABLE_NAME)
