SELECT zpu."Zone", zdo."Zone", SUM(t."total_amount") / COUNT(t."total_amount")
FROM yellow_taxi_trips t JOIN zones zpu
ON t."PULocationID"=zpu."LocationID"
JOIN zones zdo
ON t."DOLocationID"=zdo."LocationID"
GROUP BY zpu."Zone", zdo."Zone"
ORDER BY SUM(t."total_amount") / COUNT(t."total_amount") DESC
LIMIT 1;