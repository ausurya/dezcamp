## Create an external table from files in GC bucket
CREATE OR REPLACE EXTERNAL TABLE `stone-timing-374019.nytaxi.fhv_tripdata`
OPTIONS (
  format = 'CSV',
  uris = ['gs://dtc_data_lake_stone-timing-374019/raw/fhv_tripdata_2019-*.csv.gz']
);

## Create a materialized non-partitioned table from external table 
CREATE OR REPLACE TABLE `stone-timing-374019.nytaxi.fhv_tripdata_non_partitoned` AS
SELECT * FROM `nytaxi.fhv_tripdata`;

## Create a materialized partitioned&clustered table from external table
CREATE OR REPLACE TABLE `stone-timing-374019.nytaxi.fhv_tripdata_partitoned` 
PARTITION BY DATE(dropoff_datetime)
CLUSTER BY dispatching_base_num AS (
  SELECT * FROM `stone-timing-374019.nytaxi.fhv_tripdata`
);

## Count of fhv vehical records for year 2019
select count(*) from `nytaxi.fhv_tripdata_non_partitoned`

## Records have both a blank (null) PUlocationID and DOlocationID in the entire dataset?
select count(*) from 'nytaxi.fhv_tripdata_non_partitioned' where PUlocationID is NULL and DOlocationID is NULL


