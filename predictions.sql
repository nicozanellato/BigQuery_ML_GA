SELECT
*
FROM
  ml.PREDICT(MODEL `yourDataset.ML_USonly`,
   (

WITH all_visitor_stats AS (
SELECT
  fullvisitorid,
  IF(COUNTIF(totals.transactions > 0 AND totals.newVisits IS NULL) > 0, 1, 0) AS purchase_on_return
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
  GROUP BY fullvisitorid
)

  SELECT
      CONCAT(fullvisitorid, '-',CAST(visitId AS STRING)) AS unique_session_id,
      purchase_on_return,
      MAX(CAST(h.eCommerceAction.action_type AS INT64)) AS latest_ecom_progress,
      IFNULL(totals.bounces, 0) AS bounces,
      IFNULL(totals.timeOnSite, 0) AS time_on_site,
      totals.pageviews,
      trafficSource.source,
      trafficSource.medium,
      channelGrouping,
      device.deviceCategory,
      device.browser,
      device.operatingSystem,
      IFNULL(geoNetwork.country, "") AS country,
      IFNULL(geoNetwork.city, "") AS city

  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
     UNNEST(hits) AS h

    JOIN all_visitor_stats USING(fullvisitorid)

  WHERE
    totals.newVisits = 1
    AND date BETWEEN '20170701' AND '20170801' # test 1 month

  GROUP BY
  unique_session_id,
  purchase_on_return,
  bounces,
  time_on_site,
  totals.pageviews,
  trafficSource.source,
  trafficSource.medium,
  channelGrouping,
  device.deviceCategory,
  device.browser,
  device.operatingSystem,
  city,
  country
)

)

ORDER BY
  predicted_will_buy_on_return_visit DESC;