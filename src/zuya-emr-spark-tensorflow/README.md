# Deployment
Install local dependencies
```
make build_env
```

Delete (if exist) the EMR master's IP under `[emr-spark-tensorflow]` section of the file `deployment/hosts

Provision S3 and bootstrap script for EMR
```
make prepare_emr_spark_tensorflow_dependency
```

Launch EMR

Update EMR master's IP under `[emr-spark-tensorflow]` section of the file `deployment/hosts```
make launch_emr_spark_tensorflow_cluster
```

Wait for EMR to enter "Waiting..." state

Update EMR master's IP under `[emr-spark-tensorflow]` section of the file `deployment/hosts`

Setup software on EMR master


Upate EMR master's IP in scripts/connect_emr_spark_tensorflow.sh 
```
make setup_emr_spark_tensorflow_cluster
```
