"""
AWS Glue Spark ETL script from S3 to Redshift
"""

# Libraries
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions

from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql.functions import (
    input_file_name, regexp_extract, when, regexp_replace,
    substring_index, lit, to_timestamp
)
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.types import LongType, StringType, DecimalType, DoubleType, TimestampType

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
temp_dir = "s3://amazonbestsellers/temp"

# Define the input path and read the Parquet files
input_path = f"s3://{s3_bucket_name}/"
df = spark.read.parquet(input_path + "*/*/*.parquet")

print(f"Total rows read from Parquet files: {df.count()}")

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

df = df.withColumn(
    "product_price",
    regexp_replace(regexp_replace(df.product_price, r"[^\d.]", ""), r"\.(?=.*\.)", "")
)
df = df.withColumn("product_price", df.product_price.cast(DecimalType(10,2)))

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
final_df = final_df.withColumn("product_num_ratings", final_df["product_num_ratings"].cast(LongType()))
final_df = final_df.withColumn("rank", final_df["rank"].cast(LongType()))
final_df = final_df.withColumn("product_star_rating", final_df["product_star_rating"].cast(DoubleType()))
final_df = final_df.withColumn("category", final_df["category"].cast(StringType()))
final_df = final_df.withColumn("product_photo", final_df["product_photo"].cast(StringType()))
final_df = final_df.withColumn("product_url", final_df["product_url"].cast(StringType()))
final_df = final_df.withColumn("currency", final_df["currency"].cast(StringType()))
final_df = final_df.withColumn("short_product_title", final_df["short_product_title"].cast(StringType()))
final_df = final_df.withColumn("product_title", final_df["product_title"].cast(StringType()))
final_df = final_df.withColumn("asin", final_df["asin"].cast(StringType()))
final_df = final_df.withColumn("fetch_timestamp", final_df["fetch_timestamp"].cast(TimestampType()))
final_df = final_df.withColumn("product_price", final_df["product_price"].cast(DecimalType(10,2)))

# Handle null values in non-nullable columns
non_nullable_columns = ["product_num_ratings", "rank", "product_star_rating", "category", "currency", "product_title", "asin", "product_price"]
final_df = final_df.na.drop(subset=non_nullable_columns)

# Making sure the columns in the final DataFrame are properly ordered to match Redshift table
final_df = final_df.select(
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
