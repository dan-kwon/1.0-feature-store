# Databricks notebook source
import re
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
today = datetime.today()

# COMMAND ----------

# engagement features
def get_last_engagement(horizon_days, paid_subscription_end_date):
  """
  A function used to pull engagement features for churned users in their last n days
 
    Parameters
    ----------
    horizon_days : int
        number of days before cancel date (e.g. total content starts X days before a user's last day)
    paid_subscription_end_date : str
        last date of a user's paid subscription
    ...    
    Returns
    -------
    spark dataframe with engagement features for every hs_user_id, grouped by days until cancel date
    
  """
  query = f"""
    select 
      f.hs_user_id,
      s.free_trial_start_date,
      s.free_trial_end_date,
      s.paid_subscription_start_date,
      s.paid_subscription_end_date,
      'day'||datediff(s.paid_subscription_end_date, f.dt) days_until_cancel,
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
      and f.dt between date_add(s.paid_subscription_end_date,-1*{horizon_days}) and s.paid_subscription_end_date
      and s.paid_subscription_end_date = '{paid_subscription_end_date}'
    where f.dt between date_add('{paid_subscription_end_date}',-1*{horizon_days}) AND '{paid_subscription_end_date}'
      and f.hs_user_id is not null
    group by 1,2,3,4,5,6"""
  return spark.sql(query)

def get_last_distinct_engagement(horizon_days, paid_subscription_end_date):
  """
  A function used to pull weekly distinct features for canceled users
 
    Parameters
    ----------
    horizon_days : int
        number of days before cancel date (e.g. total content starts X days before a user's last day)
    paid_subscription_end_date : str
        last date of a user's paid subscription
    ...    
    Returns
    -------
    spark dataframe with engagement features for every hs_user_id, grouped by days until cancel date
    
  """
  query = f"""
    select 
      f.hs_user_id,
      s.free_trial_start_date,
      s.free_trial_end_date,
      s.paid_subscription_start_date,
      s.paid_subscription_end_date,
      'week'||(floor(datediff(s.paid_subscription_end_date, f.dt)/7)+1)::string weeks_until_cancel,
      count(distinct f.content_type) distinct_weekly_content_types,
      count(distinct f.content_id) distinct_weekly_content_titles,
      count(distinct f.playlist_id) distinct_weekly_playlists,
      count(distinct f.device_id) distinct_weekly_devices
    from silver.fact_content_consumption f
    inner join silver.fact_subscription s
      on f.hs_user_id = s.hs_user_id
      and f.dt between date_add(s.paid_subscription_end_date,-1*{horizon_days}) and s.paid_subscription_end_date
      and s.paid_subscription_end_date = '{paid_subscription_end_date}'
    where f.dt between date_add('{paid_subscription_end_date}',-1*{horizon_days}) and '{paid_subscription_end_date}'
      and f.hs_user_id is not null
    group by 1,2,3,4,5,6"""
  return spark.sql(query)

# COMMAND ----------

# event features
def get_last_events(horizon_days, paid_subscription_end_date):
  """
  A function used to pull event features for canceled users
 
    Parameters
    ----------
    horizon_days : int
        number of days before cancel date (e.g. total content starts X days before a user's last day)
    paid_subscription_end_date : str
        last date of a user's paid subscription
    ...    
    Returns
    -------
    spark dataframe with engagement features for every hs_user_id, grouped by days until cancel date
    
  """
  query = f"""
    with day_range as (
      select explode(sequence(0, {horizon_days}, 1)) day_number
    ) 
    select 
      a.hs_user_id,
      s.free_trial_start_date,
      s.free_trial_end_date,
      s.paid_subscription_start_date,
      s.paid_subscription_end_date,
      'day'||datediff(s.paid_subscription_end_date, a.client_dt) days_until_cancel,
      count(case when lower(a.event_type) = 'app start' then 1 else null end) app_starts,
      count(case when lower(a.event_type) = 'email opens' then 1 else null end) email_opens,
      count(case when lower(a.event_type) = 'email clicks' then 1 else null end) email_clicks,
      count(case when lower(a.event_type) = 'button clickthrough' then 1 else null end) button_clickthroughs,
      count(case when lower(a.event_type) = 'subscription amended' then 1 else null end) subscription_amended,
      count(case when lower(a.event_type) = 'survey start' then 1 else null end) survey_starts,
      count(distinct case when lower(a.event_type) = 'app start' then a.client_dt else null end) active_days_app_start,
      count(distinct case when lower(a.event_type) like '%viewed%screenview%' then event_type else null end) distinct_screens_viewed,
      count(distinct case when lower(a.event_type) like '%viewed%tab%screenview%' then event_type else null end) distinct_tabs_viewed
    from bronze.amplitude a
    inner join silver.fact_subscription s
      on a.hs_user_id = s.hs_user_id
      and a.client_dt between date_add(s.paid_subscription_end_date,-1*{horizon_days}) and s.paid_subscription_end_date
      and s.paid_subscription_end_date = '{paid_subscription_end_date}'
    right join day_range d
      on datediff(s.paid_subscription_end_date, a.client_dt) = d.day_number
    where a.client_dt between date_add('{paid_subscription_end_date}',-1*{horizon_days}) and '{paid_subscription_end_date}'
      and a.hs_user_id is not null
    group by 1,2,3,4,5,6"""
  return spark.sql(query)

def get_last_distinct_events(horizon_days, paid_subscription_end_date):
  """
  A function used to pull event features for new users
 
    Parameters
    ----------
    horizon_days : int
        number of days before cancel date (e.g. total content starts X days before a user's last day)
    paid_subscription_end_date : str
        last date of a user's paid subscription
    ...    
    Returns
    -------
    spark dataframe with engagement features for every hs_user_id, grouped by days until cancel date
    
  """
  query = f"""
    select 
      a.hs_user_id,
      s.free_trial_start_date,
      s.free_trial_end_date,
      s.paid_subscription_start_date,
      s.paid_subscription_end_date,
      'week'||(floor(datediff(s.paid_subscription_end_date, a.client_dt)/7)+1)::string weeks_until_cancel,
      count(distinct case when lower(a.event_type) like '%viewed%screenview%' then event_type else null end) distinct_screens_viewed,
      count(distinct case when lower(a.event_type) like '%viewed%tab%screenview%' then event_type else null end) distinct_tabs_viewed
    from bronze.amplitude a
    inner join silver.fact_subscription s
      on a.hs_user_id = s.hs_user_id
      and a.client_dt between date_add(s.paid_subscription_end_date,-1*{horizon_days}) and s.paid_subscription_end_date
      and s.paid_subscription_end_date = '{paid_subscription_end_date}'
    where a.client_dt between date_add('{paid_subscription_end_date}',-1*{horizon_days}) and '{paid_subscription_end_date}'
      and a.hs_user_id is not null
    group by 1,2,3,4,5,6
  """
  return spark.sql(query)
