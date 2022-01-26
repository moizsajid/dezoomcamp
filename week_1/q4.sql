SELECT tpep_pickup_datetime::date 
FROM yellow_taxi_trips
WHERE tip_amount = (
    SELECT MAX(tip_amount)
    FROM yellow_taxi_trips
);