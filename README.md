# Data Engineering Nanodegree Capstone Project: OpenDotA ETL
Jesse Fredrickson

www.medium.com

3/1/2020

## Purpose
The goal of this project is to transform data scraped from the OpenDotA API spanning millions of DotA matches into queriable SQL tables. The source data, which I downloaded from https://blog.opendota.com/2017/03/24/datadump2/, is comprised of 3 csv format files, and takes up 8.60 Gb. Documentation on the fields in each source file can be found here: https://docs.opendota.com/

## Method
The entire project process and schema is discussed an associated medium post, linked above. I first use dask and pandas to convert the largest of the 3 csv files, which occupies 4.0gb, to parquet format. I then upload all of the source files to an S3 bucket, and read each into a data catalog collected by two crawlers which I define using AWS Glue. Next, I define an S3 VPC endpoint to allow my S3 bucket data to be loaded into my VPC. I then spin up a Redshift cluster and create two destination tables for my data (sql statements defined in create_tables.txt). Finally, I write and run two Glue ETL jobs which load the data into redshift.
