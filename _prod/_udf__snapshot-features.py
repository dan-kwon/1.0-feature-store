# Databricks notebook source
import re
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
today = datetime.today()

# COMMAND ----------

# get longest streak
def get_longest_streak(horizon_weeks, snapshot_date):
  query = f"""
    with content_consumption as (
        select distinct hs_user_id, dt
        from silver.fact_content_consumption
        where dt between date_add('{snapshot_date}',-7*{horizon_weeks} - 1) and date_add('{snapshot_date}',-1)
          and hs_user_id is not null
          and upper(hs_user_id) like 'HSUSER%'
    ),
    content_consumption_w_lag as (
        select hs_user_id,
               dt,
               case when dt = date_add(lag(dt) over(partition by hs_user_id order by dt),1) 
                    then 0
                    else 1
                    end new_streak
        from content_consumption
    ),
    streaks as (
        select s.*,
               sum(new_streak) over(partition by hs_user_id order by dt) streak_no
        from content_consumption_w_lag s
    ), 
    all_streaks as (
        select hs_user_id,
               streak_no,
               count(distinct dt) streak_len
        from streaks
        group by 1,2
    )
    select hs_user_id,
           '{snapshot_date}' snapshot_date,
           max(streak_len) longest_streak_last{horizon_weeks}weeks
    from all_streaks
    group by 1,2
    """
  return spark.sql(query)

# COMMAND ----------

# get days since last playback
def get_days_since_last_playback(horizon_weeks, snapshot_date):
  query = f"""
    with content_consumption as (
        select distinct hs_user_id, dt
        from silver.fact_content_consumption
        where dt between date_add('{snapshot_date}',-7*{horizon_weeks}-1) and date_add('{snapshot_date}',-1)
          and hs_user_id is not null
          and upper(hs_user_id) like 'HSUSER%'
    )
    select hs_user_id,
           '{snapshot_date}' snapshot_date,
           datediff('{snapshot_date}', max(dt)) days_since_last_playback
    from content_consumption
    group by 1,2
    """
  return spark.sql(query)

# COMMAND ----------

# engagement features
def get_engagement(horizon_weeks, snapshot_date):
  """
  A function used to pull engagement features for users that exist at a certain snapshot_date
 
    Parameters
    ----------
    horizon_days : int
        cumaltive window of days to aggregate feature over (e.g. total content starts from X days prior to snapshot date to snapshot date)
    snapshot : str
        date from which to create aggregate windows for features
    ...    
    Returns
    -------
    spark dataframe with engagement features, grouped by hs_user_id
    
  """
  query = f"""
    with week_range as (
      select explode(sequence(1, {horizon_weeks}, 1)) week_number
    )
    select 
      f.hs_user_id,
      '{snapshot_date}' as snapshot_date,
      --'week'||(floor(datediff('{snapshot_date}',f.dt)/7)+1) weeks_before_snapshot,
      'last_'||week_number||'w' weeks_before_snapshot,
      count(distinct f.content_type) distinct_daily_content_types,
      count(distinct f.content_id) distinct_daily_content_titles,
      count(distinct f.playlist_id) distinct_daily_playlists,
      count(distinct f.device_id) distinct_daily_devices,
      count(distinct f.content_start_time) content_starts,
      sum(playback_ms)::float/60000 playback_minutes,
      sum(duration_ms)::float/60000 duration_minutes,
      avg(content_percentage_consumed)::float/100 avg_content_percentage_consumed,
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
    right join week_range w
      on datediff('{snapshot_date}', f.dt) <= w.week_number*7
      and w.week_number in (1,4,8,26,52)
    where f.dt between date_add('{snapshot_date}',-7*{horizon_weeks}-1) and date_add('{snapshot_date}',-1)
      and f.hs_user_id is not null
      and upper(f.hs_user_id) like 'HSUSER%'
    group by 1,2,3"""
  return spark.sql(query)

# weekly distinct engagement
def get_weekly_distinct_engagement(horizon_weeks, snapshot_date):
  """
  A function used to pull engagement features for users that exist at a certain snapshot_date
 
    Parameters
    ----------
    horizon_days : int
        cumaltive window of days to aggregate feature over (e.g. total content starts from X days prior to snapshot date to snapshot date)
    snapshot : str
        date from which to create aggregate windows for features
    ...    
    Returns
    -------
    spark dataframe with engagement features, grouped by hs_user_id
    
  """
  
  #if horizon_days % 7 == 0:
  #  adj_horizon_days = horizon_days
  #else:
  #  adj_horizon_days = horizon_days + (7 - horizon_days % 7)
    
  query = f"""
    with week_range as (
      select explode(sequence(1, {horizon_weeks}, 1)) week_number
    )
    select 
      f.hs_user_id,
      '{snapshot_date}' as snapshot_date,
      --'week'||(floor(datediff('{snapshot_date}',f.dt)/7)+1) weeks_before_snapshot,
      'last_'||week_number||'w' weeks_before_snapshot,
      count(distinct f.content_type) distinct_weekly_content_types,
      count(distinct f.content_id) distinct_weekly_content_titles,
      count(distinct f.playlist_id) distinct_weekly_playlists,
      count(distinct f.device_id) distinct_weekly_devices
    from silver.fact_content_consumption f
    right join week_range w
      on datediff('{snapshot_date}', f.dt) <= w.week_number*7
      and w.week_number = 52
    where f.dt between date_add('{snapshot_date}', -7*{horizon_weeks}-1) and date_add('{snapshot_date}',-1)
      and f.hs_user_id is not null
      and upper(f.hs_user_id) like 'HSUSER%'
    group by 1,2,3"""
  return spark.sql(query)

# COMMAND ----------

# event features
def get_amplitude_events(horizon_weeks, snapshot_date):
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
    with week_range as (
      select explode(sequence(1, {horizon_weeks}, 1)) week_number
    ) 
    select 
      a.hs_user_id,
      '{snapshot_date}' snapshot_date,
      'last_'||week_number||'w' weeks_before_snapshot,
      count(case when lower(a.event_type) = 'app start' then 1 else null end) app_starts,
      count(case when lower(a.event_type) = 'button clickthrough' then 1 else null end) button_clickthroughs,
      count(case when lower(a.event_type) = 'subscription amended' then 1 else null end) subscription_amended,
      count(case when lower(a.event_type) = 'survey start' then 1 else null end) survey_starts,
      count(distinct case when lower(a.event_type) = 'app start' then a.client_dt else null end) active_days_app_start,
      count(distinct case when lower(a.event_type) like '%viewed%screenview%' then event_type else null end) distinct_screens_viewed,
      count(distinct case when lower(a.event_type) like '%viewed%tab%screenview%' then event_type else null end) distinct_tabs_viewed
    from bronze.amplitude a
    right join week_range w
      on datediff('{snapshot_date}', a.client_dt) <= w.week_number*7
      and w.week_number in (1,4,8,26,52)
    where a.client_dt between date_add('{snapshot_date}', -7*{horizon_weeks}-1) and date_add('{snapshot_date}',-1)
      and a.hs_user_id is not null
      and upper(a.hs_user_id) like 'HSUSER%'
    group by 1,2,3"""
  return spark.sql(query)

def get_distinct_amplitude_events(horizon_weeks, snapshot_date):
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
    with week_range as (
      select explode(sequence(1, {horizon_weeks}, 1)) week_number
    ) 
    select 
      a.hs_user_id,
      '{snapshot_date}' snapshot_date,
      --'week'||(floor(datediff('{snapshot_date}', a.client_dt)/7)+1)::string weeks_before_snapshot,
      'last_'||week_number||'w' weeks_before_snapshot,
      count(distinct case when lower(a.event_type) like '%viewed%screenview%' then event_type else null end) distinct_screens_viewed,
      count(distinct case when lower(a.event_type) like '%viewed%tab%screenview%' then event_type else null end) distinct_tabs_viewed
    from bronze.amplitude a
    right join week_range w
      on datediff('{snapshot_date}', a.client_dt) <= w.week_number*7
      and w.week_number = 52
    where a.client_dt between date_add('{snapshot_date}',-7*{horizon_weeks}-1) and date_add('{snapshot_date}',-1)
      and a.hs_user_id is not null
      and upper(a.hs_user_id) like 'HSUSER%'
    group by 1,2,3
  """
  return spark.sql(query)

# COMMAND ----------

def get_braze_events(horizon_weeks, snapshot_date):
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
    with week_range as (
      select explode(sequence(1, {horizon_weeks}, 1)) week_number
    ) 
    select 
      b.external_user_id hs_user_id,
      '{snapshot_date}' snapshot_date,
      'last_'||week_number||'w' weeks_before_snapshot,
      count(case when b.event_name = 'users_messages_email_delivery' then 1 else null end) emails_recieved,
      count(case when b.event_name = 'users_messages_email_open' then 1 else null end) emails_opened,
      count(case when b.event_name = 'users_messages_email_click' then 1 else null end) emails_clicked,
      count(case when b.event_name = 'users_messages_email_unsubscribe' then 1 else null end) emails_unsubscribed,
      count(case when b.event_name = 'users_messages_pushnotification_send' then 1 else null end) pushnotifications_recieved,
      count(case when b.event_name = 'users_messages_pushnotification_open' then 1 else null end) pushnotifications_opened
    from bronze.braze_events b
    right join week_range w
      on datediff('{snapshot_date}', b.dt) <= w.week_number*7
      and w.week_number in (1,4,8,26,52)
    where b.dt between date_add('{snapshot_date}', -7*{horizon_weeks}-1) and date_add('{snapshot_date}',-1)
      and b.external_user_id is not null
      and upper(b.external_user_id) like 'HSUSER%'
    group by 1,2,3"""
  return spark.sql(query)
