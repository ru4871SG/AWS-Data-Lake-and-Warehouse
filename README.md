# AWS Data Lake and Warehouse

This repository showcase my skill to build an efficient data pipeline using AWS services. The first Lambda function `lambda_fetch_rapidapi_to_s3.py` extracts data from RapidAPI, and store it to S3 bucket (that we use as a data lake storage). Just by using this extracted data in S3, we can already query it directly with Amazon Athena. 

Then, the first AWS Glue ETL job script `glue_job_file_organizer.py` will organize the Parquet files in S3 bucket into their own category subfolders. The second AWS Glue ETL job script `glue_job_s3_to_redshift.py` will then perform proper ETL steps to load the data from S3 bucket to Amazon Redshift.

Since the ETL process from S3 to Redshift requires the usage of temporary directory, I also include the second Lambda function `lambda_delete_temp_dir_data.py` to always delete the files in the temporary directory that we use in `glue_job_s3_to_redshift.py`.

The entire pipeline is orchestrated using AWS Step Functions, where the state machine is defined in `step_functions.asl.json`. The state machine will trigger the above Lambda functions and AWS Glue ETL jobs in the correct order. This state machine is attached to EventBridge rule, so that it can be triggered periodically.

I'll provide the full details later.