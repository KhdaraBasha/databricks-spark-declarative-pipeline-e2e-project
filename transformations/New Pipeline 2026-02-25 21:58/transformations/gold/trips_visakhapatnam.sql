CREATE OR REPLACE VIEW transportation.gold.vw_trips_visakhapatnam_fact
AS (
    SELECT *
    FROM transportation.gold.vw_trip_facts
    WHERE city_name = 'Visakhapatnam'
);