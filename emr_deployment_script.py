#!/usr/bin/env python3
"""
EMR Deployment Script for Immigration ETL - Enhanced Version

This script uploads the necessary files to S3, creates an EMR cluster,
runs the immigration ETL pipeline, monitors execution, captures stdout,
and automatically terminates the cluster on successful completion.
"""
import boto3
import os
import sys
import time
import logging
import json
import argparse
from datetime import datetime
import traceback

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('backup/emr_deployment.log'),
        logging.StreamHandler(sys.stdout)  # Ensure output goes to stdout for PyCharm console
    ]
)
logger = logging.getLogger(__name__)

# Parse command line arguments
parser = argparse.ArgumentParser(description='EMR Deployment and Monitoring for Immigration ETL')
parser.add_argument('--bucket', type=str, help='S3 bucket name (overrides default)')
parser.add_argument('--prefix', type=str, help='S3 prefix/folder (overrides default)')
parser.add_argument('--region', type=str, help='AWS region (overrides default)')
parser.add_argument('--instance-type', type=str, help='EMR instance type (overrides default)')
parser.add_argument('--instance-count', type=int, help='Number of instances (overrides default)')
parser.add_argument('--keep-cluster', action='store_true', help='Do not terminate cluster after completion')
parser.add_argument('--deploy-only', action='store_true', help='Deploy cluster but do not monitor')
parser.add_argument('--monitor-only', type=str, help='Only monitor an existing cluster ID')
parser.add_argument('--etl-script', type=str, help='Path to ETL script (overrides default)')
args = parser.parse_args()

# Configuration - Update these values or override with arguments
AWS_REGION = "us-east-1"
BUCKET_NAME = "scottish-james-bucket-12345"
S3_PREFIX = "immigration-etl/"
S3_SCRIPT_PREFIX = "scripts/"
LOCAL_ETL_SCRIPT = "emr_immigration_etl.py"
LOCAL_STAGING_PATH = "staging"
EMR_CLUSTER_NAME = "ImmigrationETL"
EMR_RELEASE_LABEL = "emr-6.9.0"
EMR_LOG_URI = "s3://scottish-james-bucket-12345/immigration-etl/logs/"
EMR_INSTANCE_TYPE = "m5.xlarge"
EMR_INSTANCE_COUNT = 3
EMR_SUBNET_ID = "subnet-059d590d881bb8af4"
EMR_EC2_KEY_NAME = "vockey"


S3_BOOTSTRAP_PREFIX = "bootstrap/"
S3_STAGING_PREFIX = "staging/"
S3_OUTPUT_PREFIX = "processed/"
S3_REJECTED_PREFIX = "rejected/"

TERMINATE_ON_SUCCESS = not args.keep_cluster
MONITOR_MODE = bool(args.monitor_only)
MONITOR_CLUSTER_ID = args.monitor_only if args.monitor_only else None
DEPLOY_ONLY = args.deploy_only

# AWS client connections
s3_client = None
emr_client = None


def load_aws_credentials():
    """Load AWS credentials from environment variables."""
    aws_access_key = os.environ.get("AWS_ACCESS_KEY_ID")
    aws_secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
    aws_session_token = os.environ.get("AWS_SESSION_TOKEN")

    if not aws_access_key or not aws_secret_key:
        logger.error("AWS credentials not found in environment variables.")
        sys.exit(1)

    return aws_access_key, aws_secret_key, aws_session_token


def initialize_aws_clients(aws_access_key, aws_secret_key, aws_session_token=None):
    """Initialize AWS client connections."""
    global s3_client, emr_client

    # Create dictionary of common parameters for client initialization
    client_kwargs = {
        'region_name': AWS_REGION,
        'aws_access_key_id': aws_access_key,
        'aws_secret_access_key': aws_secret_key,
    }

    # Add session token only if it's not empty
    if aws_session_token:
        client_kwargs['aws_session_token'] = aws_session_token

    s3_client = boto3.client('s3', **client_kwargs)
    emr_client = boto3.client('emr', **client_kwargs)

    logger.info("AWS clients initialized")


def check_s3_bucket():
    """Check if S3 bucket exists and is accessible."""
    try:
        s3_client.head_bucket(Bucket=BUCKET_NAME)
        logger.info(f"Bucket {BUCKET_NAME} exists and is accessible")
        return True
    except Exception as e:
        logger.error(f"Error accessing bucket {BUCKET_NAME}: {str(e)}")
        return False


def upload_file_to_s3(local_path, s3_key):
    """Upload a file to S3."""
    try:
        logger.info(f"Uploading {local_path} to s3://{BUCKET_NAME}/{s3_key}")
        print(f"Uploading {local_path} to s3://{BUCKET_NAME}/{s3_key}")
        s3_client.upload_file(local_path, BUCKET_NAME, s3_key)
        return True
    except Exception as e:
        logger.error(f"Error uploading {local_path} to S3: {str(e)}")
        return False


def upload_staging_files():
    """Upload staging files to S3 with proper organization."""
    if not os.path.exists(LOCAL_STAGING_PATH):
        logger.error(f"Staging directory not found: {LOCAL_STAGING_PATH}")
        return False

    # Upload all files in the staging directory
    csv_count = 0
    parquet_count = 0
    other_count = 0

    for root, dirs, files in os.walk(LOCAL_STAGING_PATH):
        for file in files:
            local_path = os.path.join(root, file)

            # Get the relative path from the staging directory
            rel_path = os.path.relpath(local_path, LOCAL_STAGING_PATH)

            # Determine file type and target location
            if file.lower().endswith('.csv'):
                # CSV files go directly to staging/
                s3_key = f"{S3_PREFIX}{S3_STAGING_PREFIX}{file}".replace("\\", "/")
                if upload_file_to_s3(local_path, s3_key):
                    csv_count += 1
            elif file.lower().endswith('.parquet'):
                # Parquet files go to staging/i94/
                s3_key = f"{S3_PREFIX}{S3_STAGING_PREFIX}i94/{file}".replace("\\", "/")
                if upload_file_to_s3(local_path, s3_key):
                    parquet_count += 1
            else:
                # Other files can go to staging/ as well
                s3_key = f"{S3_PREFIX}{S3_STAGING_PREFIX}{file}".replace("\\", "/")
                if upload_file_to_s3(local_path, s3_key):
                    other_count += 1

    total_count = csv_count + parquet_count + other_count
    if total_count == 0:
        logger.warning(f"No files found in {LOCAL_STAGING_PATH}")
        return False

    logger.info(f"Uploaded {csv_count} CSV files to {S3_PREFIX}{S3_STAGING_PREFIX}")
    logger.info(f"Uploaded {parquet_count} parquet files to {S3_PREFIX}{S3_STAGING_PREFIX}i94/")
    if other_count > 0:
        logger.info(f"Uploaded {other_count} other files to {S3_PREFIX}{S3_STAGING_PREFIX}")

    return True


def upload_etl_script():
    """Upload the ETL script to S3."""
    if not os.path.exists(LOCAL_ETL_SCRIPT):
        logger.error(f"ETL script not found: {LOCAL_ETL_SCRIPT}")
        return None

    s3_key = f"{S3_PREFIX}{S3_SCRIPT_PREFIX}{os.path.basename(LOCAL_ETL_SCRIPT)}"
    if upload_file_to_s3(LOCAL_ETL_SCRIPT, s3_key):
        logger.info(f"ETL script uploaded to s3://{BUCKET_NAME}/{s3_key}")
        return s3_key
    return None


def create_bootstrap_script():
    """Create and upload a custom bootstrap script to S3."""
    # Create a local bootstrap script
    bootstrap_script = """#!/bin/bash
set -ex

echo "======================================"
echo "IMMIGRATION ETL BOOTSTRAP SCRIPT START"
echo "======================================"

# Install only essential packages needed for the ETL job
echo "Installing Python packages..."
pip3 install --user boto3==1.26.* s3fs==2023.1.0 fsspec==2023.1.0

# Create temporary directory with appropriate permissions
echo "Setting up directories..."
sudo mkdir -p /tmp/immigration-etl
sudo chmod 777 /tmp/immigration-etl

# Set up logging directory
sudo mkdir -p /var/log/immigration-etl
sudo chmod 777 /var/log/immigration-etl

# Test S3 connectivity and check for required files
echo "Testing S3 connectivity..."
aws s3 ls s3://%BUCKET_NAME%/%S3_PREFIX%%S3_STAGING_PREFIX%
if [ $? -ne 0 ]; then
    echo "ERROR: Cannot access S3 bucket. Check IAM permissions."
    exit 1
fi

# Check if lookup files exist
echo "Checking for lookup files..."
for file in i94cntyl.csv i94prtl.csv i94model.csv i94addrl.csv; do
    aws s3 ls s3://%BUCKET_NAME%/%S3_PREFIX%%S3_STAGING_PREFIX%$file
    if [ $? -ne 0 ]; then
        echo "WARNING: Lookup file $file not found!"
    else
        echo "Lookup file $file found."
    fi
done

# Check if parquet files exist in the i94 directory
echo "Checking for parquet files in i94 directory..."
aws s3 ls s3://%BUCKET_NAME%/%S3_PREFIX%%S3_STAGING_PREFIX%i94/
if [ $? -ne 0 ]; then
    echo "WARNING: i94 directory not found or empty! This might cause ETL job failure."
else
    # Count parquet files
    parquet_count=$(aws s3 ls s3://%BUCKET_NAME%/%S3_PREFIX%%S3_STAGING_PREFIX%i94/ | grep -c .parquet)
    echo "Found $parquet_count parquet files in i94 directory."
fi

# Ensure output directories exist
echo "Creating output directories if they don't exist..."

echo "======================================"
echo "IMMIGRATION ETL BOOTSTRAP COMPLETED"
echo "======================================"
"""

    # Replace placeholders with actual values
    bootstrap_script = bootstrap_script.replace("%BUCKET_NAME%", BUCKET_NAME)
    bootstrap_script = bootstrap_script.replace("%S3_PREFIX%", S3_PREFIX)
    bootstrap_script = bootstrap_script.replace("%S3_STAGING_PREFIX%", S3_STAGING_PREFIX)

    # Write the bootstrap script to a local file
    bootstrap_file = "backup/install_python_libraries.sh"
    os.makedirs("backup", exist_ok=True)  # Ensure the backup directory exists

    with open(bootstrap_file, "w") as f:
        f.write(bootstrap_script)

    # Upload the bootstrap script to S3
    s3_key = f"{S3_PREFIX}{S3_BOOTSTRAP_PREFIX}install_python_libraries.sh"
    if upload_file_to_s3(bootstrap_file, s3_key):
        logger.info(f"Bootstrap script uploaded to s3://{BUCKET_NAME}/{s3_key}")
        return f"s3://{BUCKET_NAME}/{s3_key}"

    return None


def ensure_s3_directories():
    """Ensure required S3 directories exist by creating empty marker files."""
    directories = [
        f"{S3_PREFIX}{S3_STAGING_PREFIX}",
        f"{S3_PREFIX}{S3_STAGING_PREFIX}i94/",
        f"{S3_PREFIX}{S3_OUTPUT_PREFIX}",
        f"{S3_PREFIX}{S3_REJECTED_PREFIX}"
    ]

    for dir_path in directories:
        marker_key = f"{dir_path}.keep"
        try:
            s3_client.put_object(Bucket=BUCKET_NAME, Key=marker_key, Body="")
            logger.info(f"Created marker for S3 directory: s3://{BUCKET_NAME}/{dir_path}")
        except Exception as e:
            logger.warning(f"Could not create marker for {dir_path}: {str(e)}")

    return True


def create_emr_cluster(script_s3_key, bootstrap_script_path):
    """Create an EMR cluster to run the ETL job."""
    logger.info(f"Creating EMR cluster: {EMR_CLUSTER_NAME}")

    try:
        # Generate a unique cluster name with timestamp
        timestamp = datetime.now().strftime('%Y%m%d-%H%M%S')
        cluster_name = f"{EMR_CLUSTER_NAME}-{timestamp}"

        # Define the S3 paths for the ETL process
        s3_input_path = f"s3://{BUCKET_NAME}/{S3_PREFIX}{S3_STAGING_PREFIX}i94/"
        s3_staging_path = f"s3://{BUCKET_NAME}/{S3_PREFIX}{S3_STAGING_PREFIX}"
        s3_output_path = f"s3://{BUCKET_NAME}/{S3_PREFIX}{S3_OUTPUT_PREFIX}"
        s3_rejected_path = f"s3://{BUCKET_NAME}/{S3_PREFIX}{S3_REJECTED_PREFIX}"

        logger.info(f"Staging path: {s3_staging_path}")
        logger.info(f"Output path: {s3_output_path}")
        logger.info(f"Rejected path: {s3_rejected_path}")

        # Set up bootstrap actions with our custom script
        bootstrap_actions = [
            {
                'Name': 'Setup Python Dependencies',
                'ScriptBootstrapAction': {
                    'Path': bootstrap_script_path,
                    'Args': []
                }
            }
        ]

        # Build the spark-submit command with proper arguments
        spark_submit_args = [
            'spark-submit',
            '--deploy-mode', 'client',  # Changed from 'cluster' to 'client'
            '--conf', 'spark.yarn.submit.waitAppCompletion=true',
            '--conf', 'spark.sql.debug.maxToStringFields=100',
            f's3://{BUCKET_NAME}/{script_s3_key}',
            '--s3-bucket', BUCKET_NAME,
            '--s3-prefix', S3_PREFIX,
            '--input-path', s3_input_path,
            '--staging-path', s3_staging_path,
            '--output-path', s3_output_path,
            '--rejected-path', s3_rejected_path
        ]

        # Cluster configuration
        cluster_config = {
            'Name': cluster_name,
            'LogUri': EMR_LOG_URI,
            'ReleaseLabel': EMR_RELEASE_LABEL,
            'Applications': [
                {'Name': 'Spark'},
                {'Name': 'Hadoop'},
                {'Name': 'Hive'}
            ],
            'Instances': {
                'InstanceGroups': [
                    {
                        'Name': 'Master nodes',
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'MASTER',
                        'InstanceType': EMR_INSTANCE_TYPE,
                        'InstanceCount': 1,
                    },
                    {
                        'Name': 'Worker nodes',
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'CORE',
                        'InstanceType': EMR_INSTANCE_TYPE,
                        'InstanceCount': max(1, EMR_INSTANCE_COUNT - 1),
                    }
                ],
                'Ec2SubnetId': EMR_SUBNET_ID,
                'KeepJobFlowAliveWhenNoSteps': True,
                'TerminationProtected': False,
            },
            'BootstrapActions': bootstrap_actions,
            'Steps': [
                {
                    'Name': 'Immigration ETL Pipeline',
                    'ActionOnFailure': 'CONTINUE',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': spark_submit_args
                    }
                }
            ],
            'VisibleToAllUsers': True,
            'JobFlowRole': 'EMR_EC2_DefaultRole',
            'ServiceRole': 'EMR_DefaultRole',
            'Tags': [
                {
                    'Key': 'Name',
                    'Value': EMR_CLUSTER_NAME
                },
                {
                    'Key': 'Purpose',
                    'Value': 'Immigration ETL'
                }
            ],
            # Add configurations to enhance Spark performance
            'Configurations': [
                {
                    'Classification': 'spark',
                    'Properties': {
                        'maximizeResourceAllocation': 'true'
                    }
                },
                {
                    'Classification': 'spark-defaults',
                    'Properties': {
                        'spark.driver.memory': '5G',
                        'spark.executor.memory': '5G',
                        'spark.dynamicAllocation.enabled': 'true',
                        'spark.shuffle.service.enabled': 'true'
                    }
                }
            ]
        }

        # Add EC2 key name if specified
        if EMR_EC2_KEY_NAME:
            cluster_config['Instances']['Ec2KeyName'] = EMR_EC2_KEY_NAME

        # Create the EMR cluster
        response = emr_client.run_job_flow(**cluster_config)
        cluster_id = response['JobFlowId']
        logger.info(f"Created EMR cluster with ID: {cluster_id}")
        return cluster_id

    except Exception as e:
        logger.error(f"Error in create_emr_cluster: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        return None


def get_step_status(cluster_id, step_id=None):
    """Get status of a specific step or the latest step if not specified."""
    try:
        # Get list of steps
        steps_response = emr_client.list_steps(
            ClusterId=cluster_id,
            StepStates=['PENDING', 'RUNNING', 'COMPLETED', 'CANCELLED', 'FAILED', 'INTERRUPTED']
        )

        if not steps_response['Steps']:
            logger.info("No steps found on cluster")
            return None, None

        # If step_id not specified, get the latest step
        if not step_id:
            step = steps_response['Steps'][0]  # Most recent step first
            step_id = step['Id']
        else:
            # Find the specific step
            step = next((s for s in steps_response['Steps'] if s['Id'] == step_id), None)
            if not step:
                logger.error(f"Step {step_id} not found")
                return None, None

        return step['Status']['State'], step_id
    except Exception as e:
        logger.error(f"Error getting step status: {e}")
        return None, None


def get_step_output(cluster_id, step_id):
    """Get the stdout of a specific step."""
    try:
        # First get the step details to find the log URI
        step_details = emr_client.describe_step(ClusterId=cluster_id, StepId=step_id)
        if 'Step' not in step_details:
            logger.error("Step details not found")
            return None

        # Extract the log URI
        cluster_info = emr_client.describe_cluster(ClusterId=cluster_id)
        log_uri = cluster_info['Cluster'].get('LogUri')

        if not log_uri:
            logger.error("Cluster does not have a Log URI configured")
            return None

        # Parse the S3 path - handle different formats (s3://, s3n://, etc.)
        if log_uri.startswith(('s3://', 's3n://', 's3a://')):
            # Strip off the protocol prefix
            for prefix in ['s3://', 's3n://', 's3a://']:
                if log_uri.startswith(prefix):
                    log_uri = log_uri[len(prefix):]
                    break

        s3_bucket = log_uri.split('/')[0]
        s3_prefix = '/'.join(log_uri.split('/')[1:]) if '/' in log_uri else ''
        s3_prefix = f"{s3_prefix}steps/{step_id}"

        logger.info(f"Looking for logs in bucket: {s3_bucket}, prefix: {s3_prefix}")

        # Connect to S3 (use the one we already have)
        # s3_client already initialized in the global scope

        # List objects to find the stdout file
        try:
            objects = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_prefix)

            stdout_key = None
            stderr_key = None
            for obj in objects.get('Contents', []):
                if 'stdout' in obj['Key']:
                    stdout_key = obj['Key']
                if 'stderr' in obj['Key']:
                    stderr_key = obj['Key']

            output = ""

            # Get the stdout file
            if stdout_key:
                response = s3_client.get_object(Bucket=s3_bucket, Key=stdout_key)
                stdout_content = response['Body'].read().decode('utf-8')
                output += f"===== STDOUT =====\n{stdout_content}\n"
            else:
                output += "===== STDOUT not found =====\n"

            # Get the stderr file if it exists
            if stderr_key:
                response = s3_client.get_object(Bucket=s3_bucket, Key=stderr_key)
                stderr_content = response['Body'].read().decode('utf-8')
                if stderr_content.strip():  # Only add if not empty
                    output += f"===== STDERR =====\n{stderr_content}\n"

            return output
        except Exception as s3_error:
            logger.error(f"Error accessing S3 logs: {s3_error}")
            return "Error retrieving logs from S3: " + str(s3_error)

    except Exception as e:
        logger.error(f"Error retrieving step output: {e}")
        return f"Error retrieving step output: {e}"


def terminate_cluster(cluster_id):
    """Terminate the EMR cluster."""
    try:
        logger.info(f"Terminating cluster {cluster_id}...")
        emr_client.terminate_job_flows(JobFlowIds=[cluster_id])
        logger.info(f"Cluster {cluster_id} termination initiated")

        # Wait for termination to begin
        time.sleep(5)

        # Check the new status
        response = emr_client.describe_cluster(ClusterId=cluster_id)
        new_status = response['Cluster']['Status']['State']
        logger.info(f"Cluster status after termination request: {new_status}")

        return True
    except Exception as e:
        logger.error(f"Failed to terminate cluster: {e}")
        return False


def monitor_emr_cluster(cluster_id):
    """Monitor EMR cluster and wait for completion."""
    logger.info(f"Monitoring EMR cluster: {cluster_id}")

    status_descriptions = {
        'STARTING': 'The cluster is starting up',
        'BOOTSTRAPPING': 'The cluster is bootstrapping',
        'RUNNING': 'The cluster is running steps',
        'WAITING': 'The cluster is waiting for work',
        'TERMINATING': 'The cluster is terminating',
        'TERMINATED': 'The cluster has terminated',
        'TERMINATED_WITH_ERRORS': 'The cluster terminated with errors'
    }

    try:
        status = None
        step_id = None
        step_status = None

        while status not in ('TERMINATED', 'TERMINATED_WITH_ERRORS'):
            # Get cluster status
            response = emr_client.describe_cluster(ClusterId=cluster_id)
            new_status = response['Cluster']['Status']['State']

            # Log if status changed
            if new_status != status:
                status = new_status
                status_msg = status_descriptions.get(status, status)
                logger.info(f"Cluster status changed to: {status} - {status_msg}")

                # If there's a state change reason, log it
                state_change_reason = response['Cluster']['Status'].get('StateChangeReason', {})
                if state_change_reason.get('Message'):
                    logger.info(f"Reason: {state_change_reason['Message']}")

            # Check step status
            new_step_status, new_step_id = get_step_status(cluster_id, step_id)

            # If we found a step and don't have one yet, or step status changed
            if new_step_id and (step_id is None or new_step_status != step_status):
                step_id = new_step_id
                step_status = new_step_status
                logger.info(f"Step {step_id} status: {step_status}")

                # If step completed or failed, get the output
                if step_status in ['COMPLETED', 'FAILED', 'CANCELLED']:
                    logger.info("Retrieving step output...")
                    output = get_step_output(cluster_id, step_id)
                    if output:
                        print("\n" + output)

                    # If step completed and termination is requested
                    if step_status == 'COMPLETED' and TERMINATE_ON_SUCCESS:
                        logger.info("Step completed successfully. Terminating cluster as requested.")
                        terminate_cluster(cluster_id)
                        # We can exit monitoring now
                        return True
                    elif step_status in ['FAILED', 'CANCELLED']:
                        logger.error(f"Step {step_status}. Not terminating cluster to allow investigation.")
                        if not TERMINATE_ON_SUCCESS:
                            return False

            # If cluster is waiting and no pending steps
            if status == 'WAITING':
                wait_count = 0
                steps_response = emr_client.list_steps(ClusterId=cluster_id, StepStates=['PENDING', 'RUNNING'])
                if not steps_response['Steps']:
                    wait_count += 1
                    if wait_count >= 3:  # Check 3 times to be sure
                        logger.info("Cluster is waiting with no pending steps.")

                        # Get final output if we haven't already
                        if step_id:
                            output = get_step_output(cluster_id, step_id)
                            if output:
                                print("\n" + output)

                        # Terminate if requested
                        if TERMINATE_ON_SUCCESS:
                            logger.info("ETL job appears to be completed. Terminating cluster as requested.")
                            terminate_cluster(cluster_id)
                        return True

            # If terminated, get final status
            if status in ('TERMINATED', 'TERMINATED_WITH_ERRORS'):
                if status == 'TERMINATED':
                    logger.info("Cluster terminated normally.")
                    return True
                else:
                    logger.error("Cluster terminated with errors.")
                    return False

            # Wait before checking again
            time.sleep(30)

        return status == 'TERMINATED'

    except Exception as e:
        logger.error(f"Error monitoring EMR cluster: {str(e)}")
        logger.error(f"Stack trace: {traceback.format_exc()}")
        return False


def main():
    """Main function to deploy and run the ETL job on EMR."""
    logger.info("Starting EMR deployment for Immigration ETL")

    try:
        # Step 1: Load AWS credentials
        aws_access_key, aws_secret_key, aws_session_token = load_aws_credentials()

        # Step 2: Initialize AWS clients
        initialize_aws_clients(aws_access_key, aws_secret_key, aws_session_token)

        # If we're only monitoring an existing cluster
        if MONITOR_MODE and MONITOR_CLUSTER_ID:
            logger.info(f"Running in monitor-only mode for cluster: {MONITOR_CLUSTER_ID}")
            success = monitor_emr_cluster(MONITOR_CLUSTER_ID)
            if success:
                logger.info("EMR monitoring completed successfully!")
                return 0
            else:
                logger.error("EMR monitoring detected failure.")
                return 1

        # Step 3: Check S3 bucket
        if not check_s3_bucket():
            logger.error("S3 bucket check failed. Exiting.")
            sys.exit(1)

        # Step 4: Upload ETL script to S3
        script_s3_key = upload_etl_script()
        if not script_s3_key:
            logger.error("Failed to upload ETL script. Exiting.")
            sys.exit(1)

        # Step 5: Create and upload bootstrap script
        bootstrap_script_path = create_bootstrap_script()
        if not bootstrap_script_path:
            logger.error("Failed to create bootstrap script. Exiting.")
            sys.exit(1)

        # Step 6: Upload staging data files (lookup CSVs and parquet data)
        # logger.info("Uploading data files to staging directory...")
        # if not upload_staging_files():
        #     logger.warning("Failed to upload some staging files. Continuing anyway, but ETL may fail.")
        # else:
        #     logger.info("Successfully uploaded staging data files.")

        # Step 7: Ensure S3 directories exist
        ensure_s3_directories()

        # Step 8: Create EMR cluster
        cluster_id = create_emr_cluster(script_s3_key, bootstrap_script_path)
        if not cluster_id:
            logger.error("Failed to create EMR cluster. Exiting.")
            sys.exit(1)

        # If deploy-only mode, exit after cluster creation
        if DEPLOY_ONLY:
            logger.info(f"Running in deploy-only mode. Cluster created: {cluster_id}")
            print(f"\nEMR Cluster {cluster_id} has been created.")
            print(f"To monitor this cluster later, run:")
            print(f"python {sys.argv[0]} --monitor-only {cluster_id}")
            return 0

        # Step 9: Monitor EMR cluster
        logger.info("Starting cluster monitoring...")
        success = monitor_emr_cluster(cluster_id)

        if success:
            logger.info("EMR deployment completed successfully!")
            return 0
        else:
            logger.error("EMR deployment failed.")
            return 1

    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
        return 0
    except Exception as e:
        logger.error(f"Unhandled exception: {str(e)}")
        logger.error(f"Stack trace: {traceback.format_exc()}")
        return 1


if __name__ == "__main__":
    # Create a banner at startup
    logger.info("=" * 60)
    logger.info("IMMIGRATION ETL EMR DEPLOYMENT".center(60))
    logger.info("=" * 60)

    sys.exit(main())