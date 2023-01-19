# Databricks notebook source
import re
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
today = datetime.today()

# COMMAND ----------

# engagement features
def get_engagement(horizon, date=today.date()):
  """
  A function used to pull engagement features for existing users
 
    Parameters
    ----------
    horizon : int
        number of days to look back and aggregate (e.g. total content starts in the last X days)
    date : str
        reference date for feature horizon period, defaults to today's date
    ...    
    Returns
    -------
    spark dataframe with engagement features, grouped by hs_user_id and days_from_sign_up
  """
  query = f"""select 
                f.hs_user_id,
                '{date}' snapshot_date,
                count(distinct f.content_type) last{horizon}d_distinct_content_types,
                count(distinct f.content_id) last{horizon}d_distinct_content_titles,
                count(distinct f.playlist_id) last{horizon}d_distinct_playlists,
                count(distinct f.device_id) last{horizon}d_distinct_devices,
                count(distinct f.content_start_time) last{horizon}d_content_starts,
                sum(playback_ms)::float/60000 last{horizon}d_playback_minutes,
                sum(duration_ms)::float/60000 last{horizon}d_duration_minutes,
                avg(content_percentage_consumed)::float/100 last{horizon}d_avg_content_percentage_consumed,
                (sum(playback_ms)::float/sum(duration_ms)::float) last{horizon}d_content_percentage_consumed,
                sum(case when content_type = 'wakeup' then playback_ms else 0 end)::float/60000 last{horizon}d_wakeup_playback_minutes,
                sum(case when content_type = 'course' then playback_ms else 0 end)::float/60000 last{horizon}d_course_playback_minutes,
                sum(case when content_type = 'sleepcast' then playback_ms else 0 end)::float/60000 last{horizon}d_sleepcast_playback_minutes,
                sum(case when content_type = 'meditation' then playback_ms else 0 end)::float/60000 last{horizon}d_meditation_playback_minutes,
                sum(case when content_type = 'wind down' then playback_ms else 0 end)::float/60000 last{horizon}d_winddown_playback_minutes,
                sum(case when content_type = 'sleep music' then playback_ms else 0 end)::float/60000 last{horizon}d_sleepmusic_playback_minutes,
                sum(case when content_type = 'mindful activity' then playback_ms else 0 end)::float/60000 last{horizon}d_mindfulactivity_playback_minutes,
                sum(case when content_type = 'soundscape' then playback_ms else 0 end)::float/60000 last{horizon}d_soundscape_playback_minutes,
                sum(case when content_type = 'workout' then playback_ms else 0 end)::float/60000 last{horizon}d_workout_playback_minutes,
                sum(case when content_type = 'focus music' then playback_ms else 0 end)::float/60000 last{horizon}d_focusmusic_playback_minutes,
                sum(case when extract(hour from content_start_time) in (0,1,2,3,22,23) then playback_ms else 0 end)::float/60000 last{horizon}d_latenight_playback_minutes,
                sum(case when extract(hour from content_start_time) in (4,5,6,7,8) then playback_ms else 0 end)::float/60000 last{horizon}d_earlymorning_playback_minutes,
                sum(case when extract(hour from content_start_time) in (9,10,11) then playback_ms else 0 end)::float/60000 last{horizon}d_latemorning_playback_minutes,
                sum(case when extract(hour from content_start_time) in (12,13,14,15,16) then playback_ms else 0 end)::float/60000 last{horizon}d_afternoon_playback_minutes,
                sum(case when extract(hour from content_start_time) in (17,18,19,20,21) then playback_ms else 0 end)::float/60000 last{horizon}d_evening_playback_minutes
              from silver.fact_content_consumption f
              where f.dt between date_sub('{date}',{horizon}) AND date_sub('{date}',1)
                and f.hs_user_id is not null
              group by 1,2"""
  return spark.sql(query)

def get_early_engagement(horizon,date=today.date()-relativedelta(days=28)):
  """
  A function used to pull engagement features for new users
 
    Parameters
    ----------
    horizon : int
        number of days from sign up date (e.g. total content starts X days from sign up)
    date : str
        first date of month for cohort of interest (e.g. '2021-01-01' would return features for users that signed up from 2021-01-01 to 2021-01-31)
    ...    
    Returns
    -------
    spark dataframe with engagement features, grouped by hs_user_id and days_from_sign_up
    
  """
  query = f"""select 
                f.hs_user_id,
                s.dt signup_date,
                s.free_trial_start_date,
                s.free_trial_end_date,
                s.paid_subscription_start_date,
                s.paid_subscription_end_date,
                'day'||datediff(f.dt, s.dt) days_from_signup,
                count(distinct f.content_type) distinct_daily_content_types,
                count(distinct f.content_id) distinct_daily_content_titles,
                count(distinct f.playlist_id) distinct_daily_playlists,
                count(distinct f.device_id) distinct_daily_devices,
                count(distinct f.content_start_time) content_starts,
                sum(playback_ms)::float/60000 playback_minutes,
                sum(duration_ms)::float/60000 duration_minutes,
                avg(content_percentage_consumed)::float/100 avg_content_percentage_consumed,
                (sum(playback_ms)::float/sum(duration_ms)::float) content_percentage_consumed,
                sum(case when content_type = 'wakeup' then playback_ms else 0 end)::float/60000 wakeup_playback_minutes,
                sum(case when content_type = 'course' then playback_ms else 0 end)::float/60000 course_playback_minutes,
                sum(case when content_type = 'sleepcast' then playback_ms else 0 end)::float/60000 sleepcast_playback_minutes,
                sum(case when content_type = 'meditation' then playback_ms else 0 end)::float/60000 meditation_playback_minutes,
                sum(case when content_type = 'wind down' then playback_ms else 0 end)::float/60000 winddown_playback_minutes,
                sum(case when content_type = 'sleep music' then playback_ms else 0 end)::float/60000 sleepmusic_playback_minutes,
                sum(case when content_type = 'mindful activity' then playback_ms else 0 end)::float/60000 mindfulactivity_playback_minutes,
                sum(case when content_type = 'soundscape' then playback_ms else 0 end)::float/60000 soundscape_playback_minutes,
                sum(case when content_type = 'workout' then playback_ms else 0 end)::float/60000 workout_playback_minutes,
                sum(case when content_type = 'focus music' then playback_ms else 0 end)::float/60000 focusmusic_playback_minutes,
                sum(case when extract(hour from content_start_time) in (0,1,2,3,22,23) then playback_ms else 0 end)::float/60000 latenight_playback_minutes,
                sum(case when extract(hour from content_start_time) in (4,5,6,7,8) then playback_ms else 0 end)::float/60000 earlymorning_playback_minutes,
                sum(case when extract(hour from content_start_time) in (9,10,11) then playback_ms else 0 end)::float/60000 latemorning_playback_minutes,
                sum(case when extract(hour from content_start_time) in (12,13,14,15,16) then playback_ms else 0 end)::float/60000 afternoon_playback_minutes,
                sum(case when extract(hour from content_start_time) in (17,18,19,20,21) then playback_ms else 0 end)::float/60000 evening_playback_minutes
              from silver.fact_content_consumption f
              inner join silver.fact_subscription s
                on f.hs_user_id = s.hs_user_id
                and f.dt between s.dt and coalesce(s.paid_subscription_end_date, s.free_trial_end_date)
                and s.dt = '{date}'
                and datediff(f.dt, s.dt) <= {horizon}
              where f.dt::date between '{date}'::date AND '{date}'::date+{horizon}
                and f.hs_user_id is not null
              GROUP BY 1,2,3,4,5,6,7"""
  return spark.sql(query)

def get_early_distinct_engagement(horizon, date=today.date()-relativedelta(days=28)):
  """
  A function used to pull engagement features for new users
 
    Parameters
    ----------
    horizon : int
        number of days from sign up date (e.g. total content starts X days from sign up)
    date : str
        first date of month for cohort of interest (e.g. '2021-01-01' would return features for users that signed up from 2021-01-01 to 2021-01-31)
    ...    
    Returns
    -------
    spark dataframe with engagement features, grouped by hs_user_id and days_from_sign_up
    
  """
  query = f"""select 
                f.hs_user_id,
                s.dt signup_date,
                s.free_trial_start_date,
                s.free_trial_end_date,
                s.paid_subscription_start_date,
                s.paid_subscription_end_date,
                'week'||(floor(datediff(f.dt, s.dt)/7)+1)::string weeks_from_signup,
                count(distinct f.content_type) distinct_weekly_content_types,
                count(distinct f.content_id) distinct_weekly_content_titles,
                count(distinct f.playlist_id) distinct_weekly_playlists,
                count(distinct f.device_id) distinct_weekly_devices
              from silver.fact_content_consumption f
              inner join silver.fact_subscription s
                on f.hs_user_id = s.hs_user_id
                and f.dt between s.dt and coalesce(s.paid_subscription_end_date, s.free_trial_end_date)
                and s.dt = '{date}'
                and datediff(f.dt, s.dt) <= {horizon}
              where f.dt::date between '{date}'::date AND '{date}'::date+{horizon}
                and f.hs_user_id is not null
              GROUP BY 1,2,3,4,5,6,7"""
  return spark.sql(query)

# COMMAND ----------

# event features
def get_events(horizon, date=today.date()):
  """
  A function used to pull event features for existing users
 
    Parameters
    ----------
    horizon : int
        number of days to look back and aggregate (e.g. total content starts in the last X days)
    date : str
        reference date for feature horizon period, defaults to today's date
    ...    
    Returns
    -------
    spark dataframe with event features, grouped by hs_user_id and days_from_sign_up
  """
  query = f"""select 
                a.hs_user_id,
                '{date}' snapshot_date,
                count(case when lower(a.event_type) = 'app start' then 1 else null end) last{horizon}d_app_starts,
                count(case when lower(a.event_type) = 'email opens' then 1 else null end) last{horizon}d_email_opens,
                count(case when lower(a.event_type) = 'email clicks' then 1 else null end) last{horizon}d_email_clicks,
                count(case when lower(a.event_type) = 'button clickthrough' then 1 else null end) last{horizon}d_button_clickthroughs,
                count(case when lower(a.event_type) = 'subscription amended' then 1 else null end) last{horizon}d_subscription_amended,
                count(case when lower(a.event_type) = 'survey start' then 1 else null end) last{horizon}d_survey_starts,
                count(distinct case when lower(a.event_type) = 'app start' then a.client_dt else null end) last{horizon}d_active_days_app_start,
                count(distinct case when lower(a.event_type) like '%viewed%screenview%' then event_type else null end) last{horizon}d_distinct_screens_viewed,
                count(distinct case when lower(a.event_type) like '%viewed%tab%screenview%' then event_type else null end) last{horizon}d_distinct_tabs_viewed
              from bronze.amplitude a
              where a.client_dt between date_sub('{date}',{horizon}) and date_sub('{date}',1)
                and a.hs_user_id is not null
              group by 1,2"""
  return spark.sql(query)

def get_early_events(horizon,date=today.date()-relativedelta(days=28)):
  """
  A function used to pull event features for new users
 
    Parameters
    ----------
    horizon : int
        number of days from sign up date (e.g. total content starts X days from sign up)
    date : str
        first date of month for cohort of interest (e.g. '2021-01-01' would return features for users that signed up from 2021-01-01 to 2021-01-31)
    ...    
    Returns
    -------
    spark dataframe with event features, grouped by hs_user_id and days_from_sign_up
    
  """
  query = f"""with day_range as (
                select explode(sequence(0, {horizon}, 1)) day_number
              ) 
              select 
                a.hs_user_id,
                s.dt signup_date,
                s.free_trial_start_date,
                s.free_trial_end_date,
                s.paid_subscription_start_date,
                s.paid_subscription_end_date,
                'day'||datediff(a.dt, s.dt) days_from_signup,
                count(case when lower(a.event_type) = 'app start' then 1 else null end) app_starts,
                count(case when lower(a.event_type) = 'email opens' then 1 else null end) email_opens,
                count(case when lower(a.event_type) = 'email clicks' then 1 else null end) email_clicks,
                count(case when lower(a.event_type) = 'button clickthrough' then 1 else null end) button_clickthroughs,
                count(case when lower(a.event_type) = 'subscription amended' then 1 else null end) subscription_amended,
                count(case when lower(a.event_type) = 'survey start' then 1 else null end) survey_starts,
                count(distinct case when lower(a.event_type) = 'app start' then a.client_dt else null end) active_days_app_start,
                count(distinct case when lower(a.event_type) like '%viewed%screenview%' then event_type else null end) distinct_screens_viewed,
                count(distinct case when lower(a.event_type) like '%viewed%tab%screenview%' then event_type else null end) distinct_tabs_viewed
              from  bronze.amplitude a
              inner join silver.fact_subscription s
                on a.hs_user_id = s.hs_user_id
                and a.client_dt between s.dt and coalesce(s.paid_subscription_end_date, s.free_trial_end_date)
                and s.dt = '{date}'
                and datediff(a.dt, s.dt) <= {horizon}
              right join day_range d
                on datediff(a.client_dt, s.dt) = d.day_number
              where a.client_dt::date between '{date}'::date AND '{date}'::date+{horizon}
                and a.hs_user_id is not null
              group by 1,2,3,4,5,6,7"""
  return spark.sql(query)

def get_early_distinct_events(horizon, date=today.date()-relativedelta(days=28)):
  """
  A function used to pull event features for new users
 
    Parameters
    ----------
    horizon : int
        number of days from sign up date (e.g. total content starts X days from sign up)
    date : str
        first date of month for cohort of interest (e.g. '2021-01-01' would return features for users that signed up from 2021-01-01 to 2021-01-31)
    ...    
    Returns
    -------
    spark dataframe with event features, grouped by hs_user_id and days_from_sign_up
    
  """
  query = f"""select 
                a.hs_user_id,
                s.dt signup_date,
                s.free_trial_start_date,
                s.free_trial_end_date,
                s.paid_subscription_start_date,
                s.paid_subscription_end_date,
                'week'||(floor(datediff(a.dt, s.dt)/7)+1)::string weeks_from_signup,
                count(distinct case when lower(a.event_type) like '%viewed%screenview%' then event_type else null end) distinct_screens_viewed,
                count(distinct case when lower(a.event_type) like '%viewed%tab%screenview%' then event_type else null end) distinct_tabs_viewed
              from bronze.amplitude a
              inner join silver.fact_subscription s
                on a.hs_user_id = s.hs_user_id
                and a.client_dt between s.dt and coalesce(s.paid_subscription_end_date, s.free_trial_end_date)
                and s.dt = '{date}'
                and datediff(a.dt, s.dt) <= {horizon}
              where a.client_dt::date between '{date}'::date AND '{date}'::date+{horizon}
                and a.hs_user_id is not null
              GROUP BY 1,2,3,4,5,6,7"""
  return spark.sql(query)

# COMMAND ----------

# returns specified category that is most recent
def get_most_recent_col(col, rolling_days, snapshot_date=today.date()):
  query = f"""SELECT 
                hs_user_id,
                '{snapshot_date}' AS snapshot_date,
                {col} as most_recent_{col}
              FROM (
                SELECT
                  hs_user_id, 
                  {col}, 
                  dt, 
                  row_number() over(partition by hs_user_id order by dt desc) as rank
                FROM silver.fact_content_consumption f
                WHERE f.dt BETWEEN '{snapshot_date}'::DATE-{rolling_days} AND '{snapshot_date}'::DATE-1
                  AND hs_user_id is not null
                  AND playback_ms > 10000
                  )
              WHERE rank = 1"""
  return spark.sql(query)

# returns specified category that has the most playback in the specified window
def get_most_playback_col(col, rolling_days, snapshot_date=today.date()):
  query = f"""SELECT 
                hs_user_id, 
                '{snapshot_date}' AS snapshot_date,
                {col} as most_playback_{col}
              FROM (
                SELECT
                  hs_user_id, 
                  {col}, 
                  row_number() over(partition by hs_user_id order by SUM(playback_ms) desc) AS rank
                FROM silver.fact_content_consumption f
                WHERE f.dt BETWEEN '{snapshot_date}'::DATE-{rolling_days} AND '{snapshot_date}'::DATE-1
                  AND hs_user_id is not null
                GROUP BY 1,2
              )
              WHERE rank = 1"""
  return spark.sql(query)

# returns specified category that is the most frequently occuring within the specified window
def get_most_freq_col(col, rolling_days, snapshot_date=today.date()):
  query = f"""SELECT 
                hs_user_id,
                '{snapshot_date}' AS snapshot_date,
                {col} as most_freq_{col}
              FROM (
                SELECT
                  hs_user_id, 
                  {col},
                  row_number() over(partition by hs_user_id order by COUNT(*) desc) AS rank
                FROM silver.fact_content_consumption f
                WHERE f.dt BETWEEN '{snapshot_date}'::DATE-{rolling_days} AND '{snapshot_date}'::DATE-1
                  AND hs_user_id is not null
                  AND playback_ms > 10000
                GROUP BY 1,2
              )
              WHERE rank = 1"""
  return spark.sql(query)
