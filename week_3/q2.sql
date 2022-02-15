-- Table create using BigQuery GUI from the csv files stored in GCS

SELECT COUNT(DISTINCT(dispatching_base_num))
FROM `dezoomcamp-339320.trips_data_all.fhv_2019`;