"""
AWS Glue Spark script to organize the Amazon Best Sellers Parquet files in an S3 bucket into subfolders based on their categories
"""

import boto3
from awsglue.transforms import *
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import input_file_name, regexp_extract

# Initialize Glue context and job
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# S3 bucket name
s3_bucket_name = 'amazonbestsellers'

# Define the path and read the Parquet files
input_path = f"s3://{s3_bucket_name}/"

df = spark.read.parquet(input_path + "amazonbestsellers_*.parquet")

# Extract category and filename from the full path
df = df.withColumn("full_path", input_file_name())
df = df.withColumn("category", regexp_extract("full_path", r"amazonbestsellers_([^_]+)_", 1))
df = df.withColumn("filename", regexp_extract("full_path", r"([^/]+)$", 1))

# Initialize S3 client
s3_client = boto3.client('s3')

# Check if a file exists in category subfolder
def file_exists_in_subfolder(bucket, prefix):
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
    return 'Contents' in response

# Process each file individually with a for loop
for row in df.select("full_path", "category", "filename").distinct().collect():
    original_file_path = row['full_path']
    category = row['category']
    filename = row['filename']
    
    # Check if file already exists in the category subfolder
    destination_prefix = f"{category}/{filename}"
    if file_exists_in_subfolder(s3_bucket_name, destination_prefix):
        print(f"{filename} already exists in the {category} subfolder. Skipping.")
        continue

    # Read the specific file
    file_df = spark.read.parquet(original_file_path)
    
    # Write the file to the category subfolder with its original name
    output_path = f"s3://{s3_bucket_name}/{destination_prefix}"
    file_df.coalesce(1).write.mode("overwrite").format("parquet").save(output_path)
    
    # Delete the original files under the prefix if they were successfully moved
    s3_prefix = original_file_path.replace(f"s3://{s3_bucket_name}/", "")
    try:
        # List all objects under the prefix
        response = s3_client.list_objects_v2(Bucket=s3_bucket_name, Prefix=s3_prefix)
        if 'Contents' in response:
            # Prepare the list of objects to delete and delete them
            delete_keys = [{'Key': obj['Key']} for obj in response['Contents']]
            s3_client.delete_objects(Bucket=s3_bucket_name, Delete={'Objects': delete_keys})
            print(f"Moved and deleted: {s3_prefix}")
    except Exception as e:
        print(f"Error deleting {s3_prefix}: {str(e)}")

print("File organization and cleanup completed successfully.")

job.commit()
