CREATE OR REPLACE VIEW transportation.gold.vw_trips_lucknow_fact
AS (
    SELECT *
    FROM transportation.gold.vw_trip_facts
    WHERE city_name = 'Lucknow'
);