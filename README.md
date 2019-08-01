# Investify Analytics

## Need
An investment advisor company, Investify, has decided to introduce more automation and monitoring to their data warehouse ETL pipelines. They also want a single source of truth store on the cloud for the publicly traded stock information so their analysts have a reliable source for their investment decisions.

- They have two data sources, one that provides daily stock price information in "csv" format and another source to provide stock information in "json" format. Stock price information has "ticker", "open", "close", "volumne", "date", etc. in it for each publicly traded stock; stock information source has full name, exchange, sector, and industry for each stock ticker. 

- Investment advisers at Investify have basic to intermediate SQL skills and they need to be able to query the stock information as needed from a central data warehouse to server their client needs. Their queries will be to analyse volume, or price of stocks on different dimensions like exchange, industry, sector, date, and stock name. They expect the data to be clean, queries to be highly performant, and the solution to be scalable for multiple users, and data over time. 

- The new solution needs to create a star schema with 1 fact table and 5 dimension tables; a data warehouse needs to available for query needs. There is an expectation of over 1 million rows of historical information with a future expectaion of 100 million rows to be stored for analysis. There is also an expectation of over 100 analysts assessing this data warehouse as part of their daily jobs. This new solution should have the ability to be scheduled automatically at 7 am every morning and finish its ETL process in less than 5 minutes. 

## Solution
Stock historical source data resides in S3 and new source data is put into S3 on a daily basis. Our technology team decided to pick Amazon Redshift for the data warehouse due to the storage, size of the data and need to ETL large amounts of data quickly. Apache Airflow was selected to automate, scheduling, and monitoring of the ETL workflow.

- Input Data Model
    There are two datasets available for our processing
    - Stock price information: S3 link is at s3://akcapstone/priceinfo 
        Price information dataset is in "csv" format and a sample line from it looks like AHH,11.5,11.5799999237061,8.49315452575684,11.25,11.6800003051758,4633900,2013-05-08; these values correspond to ticker,open,close,adj_close,low,high,volume,date values of a single publicly traded stock.
    - Stock descriptive information: S3 link is at s3:/akcapstone/descinfo
        Stock descriptive information dataset is in "json" format and a sample record from it looks like {"ticker":"PIH","exchange":"NASDAQ","name":"1347 PROPERTY INSURANCE HOLDINGS, INC.","sector":"FINANCE","industry":"PROPERTY-CASUALTY INSURERS"}. The ticker in each record will have price information for multiple dates in the other file.
- ETL Process
![Pipeline](https://lejmtg.dm.files.1drv.com/y4mKL0aZcXztMBq3PjsCelHj0FwYzZzBzFyurFKiwZ3ABQttUSVgwLEAKucA953GlLSGFe4ehNU_duNFo3paohFZVoLxdqqtYLJKqjCavzL0P51gZNAawyKCmdvIZp0MTTQr73ut49ePFkZ9HZ2BFknh6Fd1RRR91qBWpHoypvVTX7J6DOMI7hrN1HUGyhPD9H-sEJcyKxTx4vqwASCYXemAA?width=1170&height=369&cropmode=none)
- Output Data Model
![Star Schema](https://lehnww.dm.files.1drv.com/y4mUb31-PzPjtt-xc8A6lYkAj8UbBKncaEX1Eq5wTskQ-2WIzuM7ttuGxnLqrP6JxTceqpRxOk_KkrPxa3sNVGZhn-i2wHw5I1Qodu2j3vj4ecBM5xWysiJPvjNozovP92zeCBu1fhfrcTZNvV_W8xvvt0tx6kDOk0OKZAqEFjE7QLkWfJDjSh5PDrRPId4oSjlXMvK7ISW4n-Zh8Kn3y3yjg?width=688&height=496&cropmode=none)

## Instructions to run code
- Start airflow 
- Setup redshift in us-east-1 region as the S3 storage account is in that region;
- Run the script provided in the create_tables.sql script in redshift database to create the two staging tables
- Setup 2 connections in airflow (1 for S3 storage called aws_credentials, and another for redshift called redshift)
- Run the dag from airflow to execute the ETL pipeline that results in creating the fact and dimension tables

## ETL test performance
- In the project, there is a zip file with the publicly traded stock data in it; this "csv" file has over 1.3 million rows of data. This project ETL pipeline was able to ETL this file completely from S3 to redshift in around 1 minute and 30 seconds. 



