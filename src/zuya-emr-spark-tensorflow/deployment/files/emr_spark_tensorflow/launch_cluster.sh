#!/bin/bash

# Requires the awscli to be set up, need to have correct default region configured

CLUSTER_NAME=$1
AWS_REGION=$2
EMR_BUCKET_NAME=$3
EC2_KEY_NAME=$4
CORE_NODE_COUNT=$5
BOOTSTRAP_SCRIPT_PATH=$6

EMR_S3_URL=s3://$EMR_BUCKET_NAME
BOOTSTRAP_SCRIPT_S3_URL=$EMR_S3_URL/deployment/bootstrap.sh
EMR_LOG_S3_URL=$EMR_S3_URL/logs/

aws emr create-cluster \
    --region $AWS_REGION \
    --name spark-tensorflow \
    --release-label emr-5.1.0 \
    --applications \
      Name=Spark \
      Name=Ganglia \
      Name=Zeppelin \
    --ec2-attributes KeyName=$EC2_KEY_NAME \
    --use-default-roles \
    --instance-groups \
      InstanceGroupType=MASTER,InstanceCount=1,InstanceType=m3.xlarge \
      InstanceGroupType=CORE,InstanceCount=$CORE_NODE_COUNT,InstanceType=m3.xlarge \
    --bootstrap-actions Path=$BOOTSTRAP_SCRIPT_S3_URL \
    --log-uri $EMR_LOG_S3_URL \
    --enable-debugging
