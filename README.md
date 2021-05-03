# AWS Glue Service And PySpark Sample App

Technical details and configuration:

https://doc.clickup.com/p/h/4e5cn-36/d84c04b69acc3bd


This app runs on AWS Glue as a Python script job.
Utilizing a crawler on AWS Glue, I constructed the 'virtual' database and 'input' table from reading an input .csv file that contains babies' first name, birth year, county, sex, and count.

Then this structured data is processed using PySpark and Awsglue python module and the aggregated average birth year of each baby(grouped by first name) is written to a .csv file in an S3 bucket.

You can copy/paste the main.py code to your AWS Glue job script and run.


