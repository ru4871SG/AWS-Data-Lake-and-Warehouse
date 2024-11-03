# AWS Data Lake and Warehouse

This repository demonstrates my skills in building an efficient data pipeline using AWS services. The first Lambda function in the repository, [lambda_fetch_rapidapi_to_s3.py`](https://github.com/ru4871SG/AWS-Data-Lake-and-Warehouse/blob/main/lambda_fetch_rapidapi_to_s3.py), extracts data from RapidAPI, and store it to S3 bucket (which we use as a data lake storage). The data stored in the S3 bucket will use a partitioned structure, so we can query it efficiently with Amazon Athena.

Then, the Glue ETL job script [glue_job_s3_to_redshift.py](https://github.com/ru4871SG/AWS-Data-Lake-and-Warehouse/blob/main/glue_job_s3_to_redshift.py) will execute an ETL job to load the data from S3 bucket to Amazon Redshift.

Since the ETL process from S3 to Redshift requires the usage of temporary directory, I also include the second Lambda function [lambda_delete_temp_dir_data.py](https://github.com/ru4871SG/AWS-Data-Lake-and-Warehouse/blob/main/lambda_delete_temp_dir_data.py) to always delete the files in the temporary directory that we use in [glue_job_s3_to_redshift.py](https://github.com/ru4871SG/AWS-Data-Lake-and-Warehouse/blob/main/glue_job_s3_to_redshift.py).

This pipeline is orchestrated using AWS Step Functions, where the state machine is defined in [step_functions.asl.json](https://github.com/ru4871SG/AWS-Data-Lake-and-Warehouse/blob/main/step_functions.asl.json). You can then attach this state machine to EventBridge.

For those who prefer using a flat structure in the S3 bucket (instead of partitioned structure), the repository includes [lambda_fetch_rapidapi_to_s3_flat_structure.py](https://github.com/ru4871SG/AWS-Data-Lake-and-Warehouse/blob/main/lambda_fetch_rapidapi_to_s3_flat_structure.py). Additionally, you can use [glue_job_file_organizer_flat_structure.py](https://github.com/ru4871SG/AWS-Data-Lake-and-Warehouse/blob/main/glue_job_file_organizer_flat_structure.py) script to organize the files in the S3 bucket into their respective categories.

Regarding IAM policies associated with all the above scripts, you can check the `IAM` folder.

And for complete details on how the entire data pipeline works, you can check [my portfolio page here](https://www.datara.io/portfolio/aws-data-lake-and-warehouse).