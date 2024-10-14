"""
AWS Glue Spark ETL script from S3 to Redshift
"""

# Libraries
import sys

from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions

from pyspark.context import SparkContext
from pyspark.sql.functions import (
    input_file_name, regexp_extract, when, regexp_replace,
    substring_index, lit
)
from pyspark.sql.types import LongType, StringType, DecimalType, DoubleType, TimestampType, IntegerType
from pyspark.sql.utils import AnalysisException

from datetime import datetime, timedelta

# Initialize Glue context and job
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'TempDir'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# S3 bucket name and Redshift connection details
s3_bucket_name = 'amazonbestsellers'
redshift_connection = "redshift-connection"
redshift_table = "amazonbestsellers_redshift"

# Temporary directory before writing to Redshift
temp_dir = args['TempDir']

# Check the current date in GMT-5 timezone, so we will only process the data for today
current_time_check = (datetime.now() - timedelta(hours=5)).strftime('%Y%m%d')

# Define the input path and read the Parquet files
input_path = f"s3://{s3_bucket_name}/"
parquet_path_pattern = input_path + f"*/amazonbestsellers_*_{current_time_check}/*.parquet"

try:
    df = spark.read.parquet(parquet_path_pattern)
    total_rows = df.count()
    print(f"Total rows read from Parquet files for date {current_time_check}: {total_rows}")
except AnalysisException as e:
    # If no matching files are found, print the message and exit
    print("There is no folder inside your S3 bucket that match today's date")
    job.commit()
    sys.exit(0)

# Extract category from the file path in S3
df = df.withColumn("full_path", input_file_name())
df = df.withColumn("category", regexp_extract("full_path", r"amazonbestsellers/([^/]+)/", 1))

# Delete rank_change_label column if it exists
if 'rank_change_label' in df.columns:
    df = df.drop("rank_change_label")

# Transform product_price and create 'currency' column
df = df.withColumn(
    "currency",
    when(df.product_price.contains("$"), lit("USD"))
    .when(df.product_price.contains("€"), lit("EUR"))
    .when(df.product_price.contains("£"), lit("GBP"))
    .otherwise(lit("Unknown"))
)

# Clean 'product_price' column
df = df.withColumn(
    "product_price",
    regexp_replace(regexp_replace(df.product_price, r"[^\d.]", ""), r"\.(?=.*\.)", "")
)

# Create 'short_product_title' column
df = df.withColumn(
    "short_product_title",
    substring_index(df.product_title, ",", 1)
)
df = df.withColumn(
    "short_product_title",
    when(
        df.short_product_title == df.product_title,
        substring_index(df.product_title, "|", 1)
    ).otherwise(df.short_product_title)
)

# Select columns for the final DataFrame
final_df = df.select(
    "product_num_ratings",
    "rank",
    "product_star_rating",
    "category",
    "product_photo",
    "product_url",
    "currency",
    "short_product_title",
    "product_title",
    "asin",
    "fetch_timestamp",
    "product_price"
)

# Cast columns to match Redshift table schema
final_df = (
    final_df.withColumn("product_num_ratings", final_df["product_num_ratings"].cast(IntegerType()))
             .withColumn("rank", final_df["rank"].cast(IntegerType()))
             .withColumn("product_star_rating", final_df["product_star_rating"].cast(DoubleType()))
             .withColumn("category", final_df["category"].cast(StringType()))
             .withColumn("product_photo", final_df["product_photo"].cast(StringType()))
             .withColumn("product_url", final_df["product_url"].cast(StringType()))
             .withColumn("currency", final_df["currency"].cast(StringType()))
             .withColumn("short_product_title", final_df["short_product_title"].cast(StringType()))
             .withColumn("product_title", final_df["product_title"].cast(StringType()))
             .withColumn("asin", final_df["asin"].cast(StringType()))
             .withColumn("fetch_timestamp", final_df["fetch_timestamp"].cast(TimestampType()))
             .withColumn("product_price", final_df["product_price"].cast(DecimalType(10, 2)))
)

# Convert DataFrame to DynamicFrame
dynamic_frame = DynamicFrame.fromDF(final_df, glueContext, "dynamic_frame")

# Write DynamicFrame to Redshift
glueContext.write_dynamic_frame.from_jdbc_conf(
    frame=dynamic_frame,
    catalog_connection=redshift_connection,
    connection_options={
        "dbtable": redshift_table,
        "database": "dev"
    },
    redshift_tmp_dir=temp_dir
)

print("Data loading completed.")

job.commit()
