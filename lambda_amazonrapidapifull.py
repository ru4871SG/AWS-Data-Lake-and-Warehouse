"""
AWS Lambda function to fetch Amazon Best Sellers data from RapidAPI and upload it to an S3 bucket in Parquet format.
"""

# Libraries
import json
import http.client
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timedelta
from io import BytesIO
import os

# Lambda handler function
def lambda_handler(event, context):
    s3 = boto3.client('s3')

    # S3 bucket name
    bucket_name = 'amazonbestsellers'

    # List of categories to fetch data for
    categories = ["toys-and-games", "software", "kitchen", "appliances", "office-products"]

    def get_amazon_data(category):
        conn = http.client.HTTPSConnection("real-time-amazon-data.p.rapidapi.com")

        # Store the RapidAPI key in Lambda environment variables
        headers = {
            'x-rapidapi-key': os.environ['RAPIDAPI_KEY'],
            'x-rapidapi-host': "real-time-amazon-data.p.rapidapi.com"
        }

        try:
            conn.request("GET", f"/best-sellers?category={category}&type=BEST_SELLERS&page=1&country=US", headers=headers)

            res = conn.getresponse()
            data = res.read()

            # Parse JSON data
            parsed_data = json.loads(data.decode("utf-8"))

            return parsed_data

        except Exception as e:
            print(f"An error occurred while fetching data for category {category}: {e}")
            return None

        finally:
            conn.close()

    # Function to process and upload the data to S3
    def process_and_upload_data(category, amazon_data):
        if amazon_data and 'data' in amazon_data and 'best_sellers' in amazon_data['data']:
            df = pd.DataFrame(amazon_data['data']['best_sellers'])

            # Add fetch_timestamp column (GMT - 5 timezone)
            current_time = datetime.now() - timedelta(hours=5)
            df['fetch_timestamp'] = current_time

            # Directly convert columns to the specified data types
            df['rank'] = pd.to_numeric(df['rank'], errors='coerce').astype('Int64')
            df['product_num_ratings'] = pd.to_numeric(df['product_num_ratings'], errors='coerce').astype('Int64')
            df['product_star_rating'] = pd.to_numeric(df['product_star_rating'], errors='coerce')
            # We use string for product_price to preserve the currency symbol
            df['product_price'] = df['product_price'].astype(str)

            # Define schema with the correct data types
            schema = pa.schema([
                ('rank', pa.int64()),
                ('asin', pa.string()),
                ('product_title', pa.string()),
                ('product_price', pa.string()),
                ('product_star_rating', pa.float64()),
                ('product_num_ratings', pa.int64()),
                ('product_url', pa.string()),
                ('product_photo', pa.string()),
                ('rank_change_label', pa.string()),
                ('fetch_timestamp', pa.timestamp('us'))
            ])

            # Convert DataFrame to PyArrow Table with the defined schema
            table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)

            # Create a buffer
            buffer = BytesIO()

            # Write the table to the buffer in Parquet format
            pq.write_table(table, buffer)

            # Reset buffer position
            buffer.seek(0)

            # Create a filename with timestamp and category in the S3 bucket
            timestamp = (datetime.now() - timedelta(hours=5)).strftime('%Y%m%d')
            file_name = f"amazonbestsellers_{category}_{timestamp}.parquet"

            # Upload the data to S3
            s3.put_object(
                Bucket=bucket_name,
                Key=file_name,
                Body=buffer.getvalue(),
                ContentType='application/octet-stream'
            )

            return f"Data successfully uploaded to {bucket_name}/{file_name}"
        else:
            return f"Failed to retrieve data for category {category} or data structure is unexpected"

    # Iterate over each category
    upload_results = []
    for category in categories:
        amazon_data = get_amazon_data(category)
        result = process_and_upload_data(category, amazon_data)
        upload_results.append(result)

    # Return the results for all categories
    return {
        'statusCode': 200,
        'body': json.dumps(upload_results)
    }
