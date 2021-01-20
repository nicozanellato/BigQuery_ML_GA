CREATE OR REPLACE MODEL `taggernauta.tag_monitor.ML_USonly_XGBoost`
OPTIONS
(
model_type='boosted_tree_classifier',
labels = ['purchase_on_return']
)
AS

SELECT
  * EXCEPT(fullVisitorId)
FROM

  (SELECT
    fullVisitorId,
    IFNULL(totals.bounces, 0) AS bounces,
    IFNULL(totals.timeOnSite, 0) AS time_on_site,
    trafficSource.source,
    trafficSource.medium,
    channelGrouping,
    device.deviceCategory,
    device.browser,
    device.operatingSystem,
    IFNULL(geoNetwork.country, "") AS country,
    IFNULL(geoNetwork.city, "") AS city
  FROM
    `bigquery-public-data.google_analytics_sample.ga_sessions_*`
  WHERE
    totals.newVisits = 1 AND geoNetwork.country = 'United States' AND geoNetwork.city !='Ashburn'
    AND date BETWEEN '20160801' AND '20170430') # test/train split - train on 9 months, leaving other 3 months for test
  JOIN
  (SELECT
    fullvisitorid,
    IF(COUNTIF(totals.transactions > 0 AND totals.newVisits IS NULL) > 0, 1, 0) AS will_buy_on_return_visit
  FROM
      `bigquery-public-data.google_analytics_sample.ga_sessions_*`
      WHERE date BETWEEN '20160801' AND '20170430' AND geoNetwork.country = 'United States'
       AND geoNetwork.city !='Ashburn'
  GROUP BY fullvisitorid)
  USING (fullVisitorId)
