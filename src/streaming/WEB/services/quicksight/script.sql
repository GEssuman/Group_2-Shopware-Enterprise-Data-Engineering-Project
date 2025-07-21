-- engagement metrics
SELECT
  session_id,
  user_id,
  device_type,
  browser,
  MIN(event_time) AS session_start,
  MAX(event_time) AS session_end,
  date_diff('second', MIN(event_time), MAX(event_time)) AS session_duration_secs,
  COUNT(*) AS total_events,
  COUNT(DISTINCT page) AS unique_pages,
  MIN(event_type) AS entry_event_type,
  MAX(event_type) AS exit_event_type
FROM weblogs_db.weblogs
WHERE date(event_time) BETWEEN <<$startdate>> AND <<$enddate>>
GROUP BY session_id, user_id, device_type, browser
ORDER BY session_start DESC;

-- session metrics
SELECT
  session_id,
  user_id,
  device_type,
  browser,
  
  -- Session start and end times
  MIN(event_time) AS session_start,
  MAX(event_time) AS session_end,
  
  -- Duration of the session in seconds
  date_diff('second', MIN(event_time), MAX(event_time)) AS session_duration_secs,
  
  -- Number of total events in the session
  COUNT(*) AS total_events,
  
  -- Number of unique pages viewed during the session
  COUNT(DISTINCT page) AS unique_pages,
  
  -- First event type (entry action)
  MIN(event_type) AS entry_event_type,
  
  -- Last event type (exit action)
  MAX(event_type) AS exit_event_type
  
FROM weblogs_db.weblogs
WHERE date(event_time) BETWEEN <<$startdate>> AND <<$enddate>>
GROUP BY session_id, user_id, device_type, browser
ORDER BY session_start DESC;


-- loyalty metrics
 WITH session_data AS (
  SELECT
    session_id,
    user_id,
    session_duration_secs,
    engagement_score
  FROM (
    SELECT 
      session_id,
      user_id,
      date_diff('second', MIN(event_time), MAX(event_time)) AS session_duration_secs,
      SUM(
        CASE
          WHEN event_type = 'Click' THEN 2
          WHEN event_type = 'View' THEN 1
          WHEN event_type = 'Scroll' THEN 1
          WHEN event_type = 'Like' THEN 3
          WHEN event_type = 'Comment' THEN 4
          ELSE 1
        END
      ) AS engagement_score
    FROM weblogs_db.weblogs 
    WHERE date(event_time) BETWEEN <<$startdate>> AND <<$enddate>>
    GROUP BY session_id, user_id
  )
),

customer_loyalty_metrics AS (
  SELECT
    customer_id,
    SUM(CASE WHEN interaction_type = 'Loyalty' THEN 1 ELSE 0 END) as loyalty_interactions,
    CASE
      WHEN COUNT(*) > 0 THEN (100.0 * SUM(CASE WHEN interaction_type = 'Loyalty' THEN 1 ELSE 0 END) / COUNT(*))
      ELSE 0
    END as customer_loyalty_rate_percent,
    AVG(rating) as avg_rating
  FROM shopware_db.crm_data
  WHERE date(timestamp) BETWEEN <<$startdate>> AND <<$enddate>>
  GROUP BY customer_id
)

SELECT
  s.session_id,
  s.user_id,
  s.session_duration_secs,
  s.engagement_score,
  COALESCE(l.loyalty_interactions, 0) as customer_loyalty_interactions,
  COALESCE(l.customer_loyalty_rate_percent, 0) as customer_loyalty_rate_percent,
  COALESCE(l.avg_rating, 0) as customer_avg_rating
FROM session_data s
LEFT JOIN customer_loyalty_metrics l ON CAST(s.user_id AS int) = l.customer_id
ORDER BY s.session_id DESC
