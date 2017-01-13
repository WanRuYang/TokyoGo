#!/bin/bash

# Record starting time
touch $HOME/.bootstrap-begin

# Install python-tensorflow
# Ubuntu/Linux 64-bit, CPU only, Python 2.7
export TF_BINARY_URL=https://storage.googleapis.com/tensorflow/linux/cpu/tensorflow-0.11.0rc2-cp27-none-linux_x86_64.whl
sudo pip install --upgrade $TF_BINARY_URL

# Install package dependencies
sudo yum update -y
sudo yum install -y postgresql-libs postgresql-devel

# Install Python dependencies
sudo pip install psycopg2 pymongo boto3

# Record ending time
touch $HOME/.bootstrap-end

