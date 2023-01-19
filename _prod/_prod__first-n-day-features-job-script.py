# Databricks notebook source
# libraries
import pandas as pd
from datetime import datetime
from pyspark.sql import functions as F
from dateutil.relativedelta import relativedelta
# global variables
today = datetime.today()

# COMMAND ----------

DATE_7DAYS_AGO  = str(today.date() - relativedelta(days=7))
DATE_28DAYS_AGO = str(today.date() - relativedelta(days=28))

# COMMAND ----------

dbutils.notebook.run("prod__first-n-day-features", 60*45, {"specified_date":                DATE_28DAYS_AGO,
                                                           "event_feature_table_name":      "ds_staging.features__events_first_n_days",
                                                           "engagement_feature_table_name": "ds_staging.features__engagement_first_n_days",
                                                           "horizon_days": "28"
                                                          })

# COMMAND ----------

dbutils.notebook.run("prod__first-n-day-features", 60*45, {"specified_date":                DATE_7DAYS_AGO,
                                                           "event_feature_table_name":      "ds_staging.features__events_first_7_days",
                                                           "engagement_feature_table_name": "ds_staging.features__engagement_first_7_days",
                                                           "horizon_days": "7"
                                                          })
