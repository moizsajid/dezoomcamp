-- Table create using BigQuery GUI from the csv files stored in GCS

SELECT COUNT(*)
FROM `dezoomcamp-339320.trips_data_all.fhv_2019`
WHERE DATE(pickup_datetime)>='2019-01-01' AND DATE(pickup_datetime)<='2019-03-31' AND (dispatching_base_num="B00987" OR dispatching_base_num="B02060" OR dispatching_base_num="B02279");