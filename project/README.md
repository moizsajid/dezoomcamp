# Project

The dataset comes from CoinMarketCap API that provides data of more than 5000 cryptocurrencies with regular updates. For this project, I plan to monitor the prices of different cryptocurrencies. The end goal of this project was to generate hourly alerts for price severe fluctuations; however, this end goal could not be achieved. 

For this project, the batch data is processed hourly using Airflow. The project makes uses of Google Cloud Storage, BigQuery and Terraform. For data transformation, pyspark is used. Dashboard is not included in this project.

To run the main.py file, it has to be included in the dags directory of AIRFLOW_HOME. For there, it can be triggered from the Airflow UI. For setting up the cloud resources, main.tf file can be used inside the terraform folder. The usual steps of running terraform apply here. The CoinMarketCap API key and terraform resource credentials are not available hence some parameters have to be manually configured.