""" AWS Cluster Setup

A simple script to allocate and release all the related resources for our project.
"""
# author Avraam Marimpis <avraam.marimpis@gmail.com>

import sys

sys.path.append("../config")
import logger

import json
import argparse
import time
import boto3
from botocore.exceptions import ClientError

log_handler = logger.get_logger(logger_name="aws")
CHECK_LOGS = False

def create_s3(dwh):
    """ Create the S3 buckets.

    Parameters
    ----------
    dwh: dict
        A dictionary containing the AWS credentials and DWH configuration as generated from the function `parse_dwh`. 
    """
    client = boto3.resource(
        "s3",
        region_name=dwh["aws"]["region"],
        aws_access_key_id=dwh["aws"]["access_key_id"],
        aws_secret_access_key=dwh["aws"]["secret_access_key"],
    )

    try:
        client.create_bucket(
            Bucket=dwh["s3"]["bucket-1"]["name"],
            CreateBucketConfiguration={"LocationConstraint": dwh["aws"]["region"]},
        )
    except Exception as err:
        global CHECK_LOGS
        CHECK_LOGS = True
        log_handler.error(err)


def release_s3(dwh):
    """ Release the S3 buckets.

    Parameters
    ----------
    dwh: dict
        A dictionary containing the AWS credentials and DWH configuration as generated from the function `parse_dwh`. 

    Returns
    -------

    """
    client = boto3.resource(
        "s3",
        region_name=dwh["aws"]["region"],
        aws_access_key_id=dwh["aws"]["access_key_id"],
        aws_secret_access_key=dwh["aws"]["secret_access_key"],
    )

    try:
        bucket = client.Bucket(dwh["s3"]["bucket-1"]["name"])

        for s3_object in bucket.objects.all():
            # path, filename = os.path.split(s3_object.key)
            bucket.delete_objects(
                Delete={"Objects": [{"Key": s3_object.key}], "Quiet": True}
            )

        response = bucket.delete()

    except Exception as err:
        global CHECK_LOGS
        CHECK_LOGS = True
        log_handler.error(err)


def create_role(dwh):
    """ Create a role to manage the Redshift cluster; assigns proper policy.
    
    Parameters
    ----------
    dwh: dict
        A dictionary containing the AWS credentials and DWH configuration as generated from the function `parse_dwh`. 

    Returns
    -------
    role: string
        The new role's Amazon Resource Name (ARN).
    """
    aws_iam = boto3.client(
        "iam",
        region_name=dwh["aws"]["region"],
        aws_access_key_id=dwh["aws"]["access_key_id"],
        aws_secret_access_key=dwh["aws"]["secret_access_key"],
    )

    try:
        resp = aws_iam.create_role(
            Path="/",
            RoleName=dwh["redshift"]["iam_role_name"],
            Description="Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {
                    "Statement": [
                        {
                            "Action": "sts:AssumeRole",
                            "Effect": "Allow",
                            "Principal": {"Service": "redshift.amazonaws.com"},
                        }
                    ],
                    "Version": "2012-10-17",
                }
            ),
        )
    except Exception as err:
        global CHECK_LOGS
        CHECK_LOGS = True
        log_handler.error(err)

    resp = aws_iam.attach_role_policy(
        RoleName=dwh["redshift"]["iam_role_name"],
        PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess",
    )["ResponseMetadata"]["HTTPStatusCode"]

    roleArn = aws_iam.get_role(RoleName=dwh["redshift"]["iam_role_name"])["Role"]["Arn"]

    return roleArn


def remove_role(dwh):
    """ Remove/release the previously created role.
    
    Parameters
    ----------
    dwh: dict
        A dictionary containing the AWS credentials and DWH configuration as generated from the function `parse_dwh`. 
    """
    
    aws_iam = boto3.client(
        "iam",
        region_name=dwh["aws"]["region"],
        aws_access_key_id=dwh["aws"]["access_key_id"],
        aws_secret_access_key=dwh["aws"]["secret_access_key"],
    )

    aws_iam.detach_role_policy(
        RoleName=dwh["redshift"]["iam_role_name"],
        PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess",
    )
    aws_iam.delete_role(RoleName=dwh["redshift"]["iam_role_name"])


def create_cluster(dwh, role):
    """ Creates the Redshift cluster.
    
    Parameters
    ----------
    dwh: dict
        A dictionary containing the AWS credentials and DWH configuration as generated from the function `parse_dwh`. 
    
    role: string
        The name of the role to be created.
    """
    global CHECK_LOGS

    aws_redshift = boto3.client(
        "redshift",
        region_name=dwh["aws"]["region"],
        aws_access_key_id=dwh["aws"]["access_key_id"],
        aws_secret_access_key=dwh["aws"]["secret_access_key"],
    )

    try:
        resp = aws_redshift.create_cluster(
            ClusterType=dwh["redshift"]["type"],
            NodeType=dwh["redshift"]["node_type"],
            NumberOfNodes=int(dwh["redshift"]["num_nodes"]),
            DBName=dwh["redshift"]["db_name"],
            ClusterIdentifier=dwh["redshift"]["identifier"],
            MasterUsername=dwh["redshift"]["db_user"],
            MasterUserPassword=dwh["redshift"]["db_pass"],
            IamRoles=[role],
        )
    except Exception as err:
        CHECK_LOGS = True
        log_handler.error(err)

    time.sleep(60 * 5)

    clusterProps = aws_redshift.describe_clusters(
        ClusterIdentifier=dwh["redshift"]["identifier"]
    )["Clusters"][0]
    aws_vpc = boto3.resource(
        "ec2",
        region_name=dwh["aws"]["region"],
        aws_access_key_id=dwh["aws"]["access_key_id"],
        aws_secret_access_key=dwh["aws"]["secret_access_key"],
    )
    try:
        vpc = aws_vpc.Vpc(id=clusterProps["VpcId"])
        defaultSg = list(vpc.security_groups.all())[0]
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp="0.0.0.0/0",
            IpProtocol="TCP",
            FromPort=int(dwh["redshift"]["db_port"]),
            ToPort=int(dwh["redshift"]["db_port"]),
        )
    except Exception as err:
        CHECK_LOGS = True
        log_handler.error(err)


def delete_cluster(dwh):
    """ Release the Redshift cluster.
    
    Parameters
    ----------
    dwh: dict
        A dictionary containing the AWS credentials and DWH configuration as generated from the function `parse_dwh`. 
    """
    aws_redshift = boto3.client(
        "redshift",
        region_name=dwh["aws"]["region"],
        aws_access_key_id=dwh["aws"]["access_key_id"],
        aws_secret_access_key=dwh["aws"]["secret_access_key"],
    )

    aws_redshift.delete_cluster(
        ClusterIdentifier=dwh["redshift"]["identifier"], SkipFinalClusterSnapshot=True
    )


if __name__ == "__main__":
    dwh = None

    try:
        with open("dwh.json") as fp:
            dwh = json.load(fp)
            log_handler.debug(dwh)
    except Exception as err:
        log_handler.error(err)
        sys.exit(1)

    parser = argparse.ArgumentParser(description="AWS resources setup.")
    parser.add_argument(
        "--create-all",
        action="store_true",
        help="Create the Redshift cluster and its associated role.",
    )
    parser.add_argument(
        "--create-s3", action="store_true", help="Create the S3 buckets."
    )
    parser.add_argument(
        "--release-s3", action="store_true", help="Release the S3 buckets."
    )
    parser.add_argument(
        "--release-role", action="store_true", help="Removes the Redshift role."
    )
    parser.add_argument(
        "--release-cluster", action="store_true", help="Delete the Redshift cluster."
    )
    parser.add_argument(
        "--release-all",
        action="store_true",
        help="Remove the Redshift role and delete the cluster itself.",
    )
    args = parser.parse_args()

    if args.release_all:
        print("Releasing all resources... ", end="")
        remove_role(dwh)
        delete_cluster(dwh)
        print("✓")

        print("Entering grace time... ", end="")
        time.sleep(60)
        print("✓")

    if args.create_all:
        print("Creating role...  ", end="")
        role = create_role(dwh)
        print("✓")

        print("Creating cluster... ", end="")
        create_cluster(dwh, role)
        print("✓")

    if args.create_s3 or args.create_all:
        print("Creating S3 buckets... ", end="")
        create_s3(dwh)
        print("✓")

    if args.release_s3 or args.release_all:
        print("Releasing S3 buckets... ", end="")
        release_s3(dwh)
        print("✓")

    if CHECK_LOGS:
        print()
        print("Something went wrong; consult the logs...")
        print()
