SELECT t."DOLocationID", zdo."Zone", count(t."index")
FROM yellow_taxi_trips t JOIN zones zpu
ON t."PULocationID"=zpu."LocationID"
JOIN zones zdo
ON t."DOLocationID"=zdo."LocationID"
WHERE zpu."Zone"='Central Park' AND tpep_pickup_datetime::date='2021-01-14'
GROUP BY t."DOLocationID", zdo."Zone"
ORDER BY count(t."index") DESC
LIMIT 1;