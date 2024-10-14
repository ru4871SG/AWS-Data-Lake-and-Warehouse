"""
AWS Lambda function to delete all files and folders in the 'temp' directory in the S3 bucket.
"""

# Library
import boto3

def lambda_handler(event, context):
    s3 = boto3.resource('s3')
    bucket_name = 'aws-glue-assets-880013172391-us-east-1'
    prefix = 'temporary/'

    bucket = s3.Bucket(bucket_name)
    
    # Delete all objects with the specified prefix
    bucket.objects.filter(Prefix=prefix).delete()

    return {
        'statusCode': 200,
        'body': 'All files and folder deleted successfully'
    }
