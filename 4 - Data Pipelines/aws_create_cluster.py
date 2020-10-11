""" AWS Cluster Setup

A simple script to allocate and release all the related resources for our project.
"""
# author Avraam Marimpis <avraam.marimpis@gmail.com>

import sys
import json
import argparse
import time
import boto3
from botocore.exceptions import ClientError
from logger import get_logger


# Root credentials
AWS_KEY = ""
AWS_SECRET = ""
AWS_REGION = "us-west-2"
# DWH configuration
DWH_CLUSTER_TYPE = "multi-node"
DWH_NUM_NODES = 2
DWH_NODE_TYPE = "dc2.large"
DWH_IAM_ROLE_NAME = "aws_iam_redshift"
DWH_CLUSTER_IDENTIFIER = "redshift-cluster-1"
DWH_DB = ""
DWH_DB_USER = "awsuser"
DWH_DB_PASSWORD = ""
DWH_PORT = 5439


l = get_logger(logger_name="aws")
CHECK_LOGS = False

def create_role():
    """ Create a role to manage the Redshift cluster; assigns proper policy.
    
    Returns
    -------
    role: string
        The new role's Amazon Resource Name (ARN).
    """
    aws_iam = boto3.client(
        'iam',
        region_name=AWS_REGION,
        aws_access_key_id=AWS_KEY,
        aws_secret_access_key=AWS_SECRET,
    )
    
    try:
        resp = aws_iam.create_role(
            Path='/',
            RoleName=DWH_IAM_ROLE_NAME,
            Description = "Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps({
                'Statement': [{
                    'Action': 'sts:AssumeRole',
                    'Effect': 'Allow',
                    'Principal': {
                        'Service': 'redshift.amazonaws.com'
                    }
                }],
                'Version': '2012-10-17'
            })
        )
    except Exception as err:
        global CHECK_LOGS
        CHECK_LOGS = True
        l.error(err)
        
    resp = aws_iam.attach_role_policy(
        RoleName=DWH_IAM_ROLE_NAME,
        PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
    )['ResponseMetadata']['HTTPStatusCode']
        
    roleArn = aws_iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
    
    return roleArn


def remove_role():
    aws_iam = boto3.client(
        'iam',
        region_name=AWS_REGION,
        aws_access_key_id=AWS_KEY,
        aws_secret_access_key=AWS_SECRET,
    )
    
    aws_iam.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
    aws_iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)

    
def create_cluster(role):
    """ Creates the Redshift cluster."""
    global CHECK_LOGS

    aws_redshift = boto3.client(
        'redshift',
        region_name=AWS_REGION,
        aws_access_key_id=AWS_KEY,
        aws_secret_access_key=AWS_SECRET
    )

    try:
        resp = aws_redshift.create_cluster(
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),
            DBName=DWH_DB,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,
            IamRoles=[role]
        )
    except Exception as err:
        CHECK_LOGS = True
        l.error(err)
        
    time.sleep(60 * 5)
        
    clusterProps = aws_redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    aws_vpc = boto3.resource(
        'ec2',
        region_name=AWS_REGION,
        aws_access_key_id=AWS_KEY,
        aws_secret_access_key=AWS_SECRET
    )
    try:
        vpc = aws_vpc.Vpc(id=clusterProps['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(DWH_PORT),
            ToPort=int(DWH_PORT)
        )
    except Exception as err:
        CHECK_LOGS = True
        l.error(err)    
    
def delete_cluster():
    aws_redshift = boto3.client(
        'redshift',
        region_name=AWS_REGION,
        aws_access_key_id=AWS_KEY,
        aws_secret_access_key=AWS_SECRET
    )
    
    aws_redshift.delete_cluster( ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)

    
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="AWS resources setup.")
    parser.add_argument("--create-all", action="store_true", help="Create the Redshift cluster and its associated role.")
    parser.add_argument("--release-role", action="store_true", help="Removes the Redshift role.")
    parser.add_argument("--release-cluster", action="store_true", help="Delete the Redshift cluster.")
    parser.add_argument("--release-all", action='store_true', help="Remove the Redshift role and delete the cluster itself.")
    args = parser.parse_args()
    
    if args.release_all:
        print("Releasing all resources... ", end="")
        remove_role()
        delete_cluster()
        print("✓")
        
        print("Entering grace time... ", end="")
        time.sleep(60)
        print("✓")
        
    if args.create_all:
        print("Creating role... ", end="")
        role = create_role()
        print("✓")

        print("Creating cluster... ", end="")
        create_cluster(role)
        print("✓")

    if CHECK_LOGS:
        print()
        print("Something went wrong; consult the logs...")
        print()
