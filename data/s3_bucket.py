import boto3
from botocore.exceptions import ClientError

REGION = "ap-southeast-1"

def checkBucketExists(s3, bucket_name):
    try:
        s3.head_bucket(Bucket=bucket_name)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchBucket"):
            return False
        raise

if __name__ == "__main__":
    s3 = boto3.client("s3", region_name=REGION)
    
    bucket_name = "zomato-pipeline-s3 "

    if not checkBucketExists(s3, bucket_name):
        s3.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": REGION},
        )
        print(f"Created bucket: {bucket_name}")

    print("Buckets:")
    for b in s3.list_buckets()["Buckets"]:
        print(" -", b["Name"])
    