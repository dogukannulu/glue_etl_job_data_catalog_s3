import boto3
import requests
import logging
import argparse
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class S3Uploader:
    def __init__(self, region='eu-central-1'):
        self.s3_client = boto3.client('s3')
        self.region = region

    def create_bucket(self, name):
        """
        Create an S3 bucket with the specified region and name
        """
        try:
            self.s3_client.create_bucket(
                Bucket=name,
                CreateBucketConfiguration={
                    'LocationConstraint': self.region
                }
            )
            logger.info(f"Bucket '{name}' created successfully.")
        except ClientError as e:
            if e.response['Error']['Code'] == 'BucketAlreadyOwnedByYou':
                logger.warning(f"Bucket '{name}' already exists and is owned by you.")
            elif e.response['Error']['Code'] == 'BucketAlreadyExists':
                logger.warning(f"Bucket '{name}' already exists and is owned by someone else.")
            else:
                logger.warning("An unexpected error occured while creating the S3 bucket:", e)

    def put_object(self, bucket_name, object_key, csv_data):
        """
        Upload the CSV data into the specified S3 bucket
        """
        try:
            self.s3_client.put_object(Body=csv_data, Bucket=bucket_name, Key=object_key)
            logger.info('Object uploaded into S3 successfully')
        except ClientError as e:
            logger.warning("An error occurred while putting the object into S3 related to ClientError:", e)
        except Exception as e:
            logger.warning("An unexpected error occurred while putting the object into S3:", e)


def define_arguments():
    """
    Defines the command-line arguments
    """
    parser = argparse.ArgumentParser(description="Upload CSV data to Amazon S3")
    parser.add_argument("--bucket_name", "-bn", required=True, help="Name of the S3 bucket")
    parser.add_argument("--object_key", "-ok", required=True, help="Name for the S3 object")
    parser.add_argument("--data_url", "-du", required=True, help="URL of the remote CSV file")
    args = parser.parse_args()

    return args


def main():
    args = define_arguments()
    csv_data = requests.get(args.data_url).content
    s3_uploader = S3Uploader()
    s3_uploader.create_bucket(name=args.bucket_name)
    s3_uploader.put_object(bucket_name=args.bucket_name, object_key=args.object_key, csv_data=csv_data)



if __name__ == '__main__':
    main()
