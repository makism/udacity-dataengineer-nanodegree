import boto3
import os

if __name__ == "__main__":
    aws_s3 = boto3.resource(
        's3',
        aws_access_key_id="",
        aws_secret_access_key=""
    )
    
    aws_bucket = aws_s3.Bucket('udacity-dend')

    for s3_object in aws_bucket.objects.all():
        path, filename = os.path.split(s3_object.key)
        
        if path.startswith("log-data/"):
            print(path, filename)
    
