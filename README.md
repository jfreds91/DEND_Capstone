# Data Engineering Nanodegree Capstone Project: OpenDotA ETL
Jesse Fredrickson

3/1/2020

## Purpose
The goal of this project is to transform data scraped from the OpenDotA API spanning millions of DotA matches into queriable SQL tables. The source data, which I downloaded from https://blog.opendota.com/2017/03/24/datadump2/, is comprised of 3 csv format files, and takes up 8.60 Gb. Documentation on the fields in each source file can be found here: https://docs.opendota.com/

## Method
I have uploaded the source data to a public S3 bucket, and I will first use Spark to read the data into staging tables, then I will create OLAP tables from those and save them into parquet format on S3. Finally, I will start up a Redshift cluster and read those parquet files into permanent SQL tables.

## Data
