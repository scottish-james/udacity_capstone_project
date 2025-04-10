#!/usr/bin/env python3
"""
EMR Setup Script - Creates all the required AWS resources for EMR deployment
"""
import boto3
import time
import os
import sys
import json

# Configuration - Use these exact values
AWS_REGION = "us-east-1"
BUCKET_NAME = "scottish-james-bucket-12345"
S3_PREFIX = "immigration-etl/"
S3_SCRIPT_PREFIX = "scripts/"
LOCAL_ETL_SCRIPT = "emr_immigration_etl.py"
LOCAL_STAGING_PATH = "staging"
EMR_CLUSTER_NAME = "ImmigrationETL"
EMR_RELEASE_LABEL = "emr-6.9.0"
EMR_LOG_URI = f"s3://{BUCKET_NAME}/{S3_PREFIX}logs/"
EMR_INSTANCE_TYPE = "m5.xlarge"
EMR_INSTANCE_COUNT = 3
EMR_SUBNET_ID = "subnet-059d590d881bb8af4"  # Using your actual subnet ID
EMR_EC2_KEY_NAME = "vockey"  # Using your EC2 key pair name

print("Setting up EMR prerequisites...")

# Get AWS credentials from environment
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")
AWS_SESSION_TOKEN = os.environ.get("AWS_SESSION_TOKEN")

if not AWS_ACCESS_KEY_ID or not AWS_SECRET_ACCESS_KEY:
    print("ERROR: AWS credentials not found in environment variables.")
    print("Please set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables.")
    sys.exit(1)

# Initialize AWS clients
session = boto3.Session(
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    aws_session_token=AWS_SESSION_TOKEN,
    region_name=AWS_REGION
)

s3 = session.client('s3')
iam = session.client('iam')

# Step 1: Create S3 bucket
print("\n1. Creating S3 bucket...")
try:
    s3.head_bucket(Bucket=BUCKET_NAME)
    print(f"   S3 bucket {BUCKET_NAME} already exists")
except Exception:
    try:
        # Create the bucket
        if AWS_REGION == "us-east-1":
            s3.create_bucket(Bucket=BUCKET_NAME)
        else:
            s3.create_bucket(
                Bucket=BUCKET_NAME,
                CreateBucketConfiguration={'LocationConstraint': AWS_REGION}
            )
        print(f"   S3 bucket created: {BUCKET_NAME}")
    except Exception as e:
        print(f"   Error creating S3 bucket: {str(e)}")

# Step 2: Create folder structure in S3
print("\n2. Creating S3 folder structure...")
folders = [
    f"{S3_PREFIX}staging/i94/",
    f"{S3_PREFIX}staging/",
    f"{S3_PREFIX}processed/",
    f"{S3_PREFIX}rejected/",
    f"{S3_PREFIX}logs/",
    f"{S3_PREFIX}{S3_SCRIPT_PREFIX}"
]
for folder in folders:
    try:
        s3.put_object(Bucket=BUCKET_NAME, Key=folder)
        print(f"   Created folder: s3://{BUCKET_NAME}/{folder}")
    except Exception as e:
        print(f"   Error creating folder {folder}: {str(e)}")

# Step 3: Create EMR default roles
print("\n3. Creating EMR default roles...")
try:
    # Check if roles exist
    try:
        iam.get_role(RoleName='EMR_DefaultRole')
        iam.get_role(RoleName='EMR_EC2_DefaultRole')
        print("   EMR default roles already exist")
    except iam.exceptions.NoSuchEntityException:
        print("   Creating EMR default roles...")
        # Service role
        trust_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"Service": "elasticmapreduce.amazonaws.com"},
                    "Action": "sts:AssumeRole"
                }
            ]
        }

        iam.create_role(
            RoleName='EMR_DefaultRole',
            AssumeRolePolicyDocument=json.dumps(trust_policy)
        )

        iam.attach_role_policy(
            RoleName='EMR_DefaultRole',
            PolicyArn='arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceRole'
        )

        # EC2 role
        ec2_trust_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"Service": "ec2.amazonaws.com"},
                    "Action": "sts:AssumeRole"
                }
            ]
        }

        iam.create_role(
            RoleName='EMR_EC2_DefaultRole',
            AssumeRolePolicyDocument=json.dumps(ec2_trust_policy)
        )

        iam.attach_role_policy(
            RoleName='EMR_EC2_DefaultRole',
            PolicyArn='arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceforEC2Role'
        )

        # Create instance profile
        iam.create_instance_profile(InstanceProfileName='EMR_EC2_DefaultRole')
        iam.add_role_to_instance_profile(
            InstanceProfileName='EMR_EC2_DefaultRole',
            RoleName='EMR_EC2_DefaultRole'
        )

        print("   EMR default roles created")
except Exception as e:
    print(f"   Error with EMR roles: {str(e)}")
    print("   You may need to run 'aws emr create-default-roles' manually.")

# Step 4: Verify EC2 key pair
print(f"\n4. Verifying EC2 key pair: {EMR_EC2_KEY_NAME}")
ec2 = session.client('ec2')
try:
    ec2.describe_key_pairs(KeyNames=[EMR_EC2_KEY_NAME])
    print(f"   EC2 key pair {EMR_EC2_KEY_NAME} exists")
except Exception as e:
    print(f"   Warning: EC2 key pair {EMR_EC2_KEY_NAME} not found: {str(e)}")
    print("   You may need to create this key pair in the EC2 console or use a different key.")

# Step 5: Verify subnet
print(f"\n5. Verifying subnet: {EMR_SUBNET_ID}")
try:
    response = ec2.describe_subnets(SubnetIds=[EMR_SUBNET_ID])
    vpc_id = response['Subnets'][0]['VpcId']
    print(f"   Subnet {EMR_SUBNET_ID} exists in VPC {vpc_id}")
except Exception as e:
    print(f"   Warning: Subnet {EMR_SUBNET_ID} not found: {str(e)}")
    print("   You may need to create this subnet or use a different subnet ID.")

# Step 6: Add explicit S3 bucket policy
print("\n6. Adding S3 bucket policy for EMR access...")
bucket_policy = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "AWS": [
                    f"arn:aws:iam::{boto3.client('sts').get_caller_identity().get('Account')}:role/EMR_EC2_DefaultRole",
                    f"arn:aws:iam::{boto3.client('sts').get_caller_identity().get('Account')}:role/EMR_DefaultRole"
                ]
            },
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject",
                "s3:ListBucket"
            ],
            "Resource": [
                f"arn:aws:s3:::{BUCKET_NAME}",
                f"arn:aws:s3:::{BUCKET_NAME}/*"
            ]
        }
    ]
}
try:
    s3.put_bucket_policy(
        Bucket=BUCKET_NAME,
        Policy=json.dumps(bucket_policy)
    )
    print("   S3 bucket policy added successfully")
except Exception as e:
    print(f"   Error adding bucket policy: {str(e)}")


# Final output
print("\n========== SETUP COMPLETE ==========")
print("\nYour EMR Configuration:")
print(f"AWS_REGION = \"{AWS_REGION}\"")
print(f"BUCKET_NAME = \"{BUCKET_NAME}\"")
print(f"S3_PREFIX = \"{S3_PREFIX}\"")
print(f"S3_SCRIPT_PREFIX = \"{S3_SCRIPT_PREFIX}\"")
print(f"LOCAL_ETL_SCRIPT = \"{LOCAL_ETL_SCRIPT}\"")
print(f"LOCAL_STAGING_PATH = \"{LOCAL_STAGING_PATH}\"")
print(f"EMR_CLUSTER_NAME = \"{EMR_CLUSTER_NAME}\"")
print(f"EMR_RELEASE_LABEL = \"{EMR_RELEASE_LABEL}\"")
print(f"EMR_LOG_URI = \"{EMR_LOG_URI}\"")
print(f"EMR_INSTANCE_TYPE = \"{EMR_INSTANCE_TYPE}\"")
print(f"EMR_INSTANCE_COUNT = {EMR_INSTANCE_COUNT}")
print(f"EMR_SUBNET_ID = \"{EMR_SUBNET_ID}\"")
print(f"EMR_EC2_KEY_NAME = \"{EMR_EC2_KEY_NAME}\"")

print("\nNext steps:")
print("1. Make sure your ETL script is saved as emr_immigration_etl.py")
print("2. Upload your ETL script to S3 with the deployment script")
