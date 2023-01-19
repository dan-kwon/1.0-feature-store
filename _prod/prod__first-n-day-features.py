# Databricks notebook source
# MAGIC %md
# MAGIC ## DS first-n-day features
# MAGIC A variety of helpful aggregated tables that hold data and features used in various data science models

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load necessary UDFs and libraries

# COMMAND ----------

# MAGIC %run "./prod__helper_library"

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
                     defaultValue=str(today.date() - relativedelta(days=28)), 
                     label="Specified Date")

dbutils.widgets.dropdown(name="horizon_days",
                         defaultValue=str(28), 
                         choices=[str(i) for i in pd.Series(np.arange(7,29,1))],
                         label="Horizon Days")

dbutils.widgets.text(name="event_feature_table_name", 
                     defaultValue="ds_staging.features__event_first_n_days", 
                     label="Destination Event Feature Table Name")

dbutils.widgets.text(name="engagement_feature_table_name", 
                     defaultValue="ds_staging.features__engagement_first_n_days", 
                     label="Destination Engagement Feature Table Name")

# COMMAND ----------

SIGNUP_DATE           = dbutils.widgets.get("specified_date")
HORIZON_DAYS          = int(dbutils.widgets.get("horizon_days"))
EVENT_TABLE_NAME      = dbutils.widgets.get("event_feature_table_name")
ENGAGEMENT_TABLE_NAME = dbutils.widgets.get("engagement_feature_table_name")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create engagement features

# COMMAND ----------

[today.date() - timedelta(days=x+28) for x in range(4)] 

# COMMAND ----------

# new users
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

for i in [SIGNUP_DATE]: #[today.date() - timedelta(days=x+28) for x in range(4)]
  signup_date = str(i)
  df1 = get_early_engagement(HORIZON_DAYS, signup_date)
  df1 = df1 \
    .groupBy("hs_user_id","signup_date") \
    .pivot("days_from_signup") \
    .agg(*[F.sum(x).alias(x) for x in df1.columns if x not in {"hs_user_id",
                                                               "signup_date",
                                                               "free_trial_start_date",
                                                               "free_trial_end_date",
                                                               "paid_subscription_start_date",
                                                               "paid_subscription_end_date",
                                                               "days_from_signup"}])
  
  df2 = get_early_distinct_engagement(HORIZON_DAYS-1, signup_date) 
  df2 = df2 \
    .groupBy("hs_user_id","signup_date") \
    .pivot("weeks_from_signup") \
    .agg(*[F.sum(x).alias(x) for x in df2.columns if x not in {"hs_user_id",
                                                               "signup_date",
                                                               "weeks_from_signup",
                                                               "free_trial_start_date",
                                                               "free_trial_end_date",
                                                               "paid_subscription_start_date",
                                                               "paid_subscription_end_date",
                                                               "days_from_signup"}])
  
  df3 = df1.join(df2, how='left', on=['hs_user_id','signup_date'])
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

for i in [SIGNUP_DATE]: #[today.date() - timedelta(days=x+28) for x in range(4)]
  signup_date = str(i)
  df1 = get_early_events(HORIZON_DAYS, signup_date)
  df1 = df1 \
    .groupBy("hs_user_id","signup_date") \
    .pivot("days_from_signup") \
    .agg(*[F.sum(x).alias(x) for x in df1.columns if x not in {"hs_user_id",
                                                               "signup_date",
                                                               "free_trial_start_date",
                                                               "free_trial_end_date",
                                                               "paid_subscription_start_date",
                                                               "paid_subscription_end_date",
                                                               "days_from_signup"}])
  
  df2 = get_early_distinct_events(HORIZON_DAYS-1, signup_date) 
  df2 = df2 \
    .groupBy("hs_user_id","signup_date") \
    .pivot("weeks_from_signup") \
    .agg(*[F.sum(x).alias(x) for x in df2.columns if x not in {"hs_user_id",
                                                               "signup_date",
                                                               "weeks_from_signup",
                                                               "free_trial_start_date",
                                                               "free_trial_end_date",
                                                               "paid_subscription_start_date",
                                                               "paid_subscription_end_date",
                                                               "days_from_signup"}])
  
  df3 = df1.join(df2, how='left', on=['hs_user_id','signup_date'])
  df3.na.fill(0) \
    .write \
    .mode('append') \
    .option("mergeSchema", "true") \
    .saveAsTable(EVENT_TABLE_NAME)

# COMMAND ----------


