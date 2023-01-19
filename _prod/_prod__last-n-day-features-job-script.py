# Databricks notebook source
# libraries
import pandas as pd
from datetime import datetime, timedelta
from pyspark.sql import functions as F
from dateutil.relativedelta import relativedelta
# global variables
today = datetime.today()

# COMMAND ----------

DATE_1DAYS_AGO = str(today.date() - relativedelta(days=1))

# COMMAND ----------

dbutils.notebook.run("prod__last-n-day-features", 60*45, {"specified_date":                DATE_1DAYS_AGO,
                                                          "event_feature_table_name":      "ds_staging.features__events_last_n_days",
                                                          "engagement_feature_table_name": "ds_staging.features__engagement_last_n_days",
                                                          "horizon_days": "56"
                                                         })

# COMMAND ----------

# backfill (comment out after running and deploying to prod)
#for i in [pd.to_datetime("2022-06-04").date() - timedelta(days=x) for x in range(3)]:
#  dbutils.notebook.run("prod__last-n-day-features", 60*45, {"specified_date":              str(i),
#                                                             "event_feature_table_name":      "ds_staging.features__events_last_n_days",
#                                                             "engagement_feature_table_name": "ds_staging.features__engagement_last_n_days",
#                                                             "horizon_days": "56"
#                                                              })
