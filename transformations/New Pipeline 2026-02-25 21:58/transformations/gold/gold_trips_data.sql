CREATE OR REFRESH MATERIALIZED VIEW transportation.gold.vw_trip_facts
AS 
(
  SELECT
    t.id,
    t.business_date,
    t.city_id,
    c.city_name,
    t.passenger_category,
    t.distance_kms,
    t.sales_amt,
    t.passenger_rating,
    t.driver_rating,
    ca.day,
    ca.week,
    ca.month,
    ca.year,
    ca.day_of_week,
    ca.quarter
  FROM transportation.silver.tb_trips_silver as t
  LEFT JOIN transportation.silver.city_view as c ON c.city_id = t.city_id
  LEFT JOIN transportation.silver.calendar_view as ca ON t.business_date = ca.date
);