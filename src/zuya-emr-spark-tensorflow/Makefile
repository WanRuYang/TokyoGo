build_env:
	pip install -r requirements.txt

prepare_emr_spark_tensorflow_dependency:
	ansible-playbook -i deployment/hosts deployment/emr_spark_tensorflow.yml -t provision

launch_emr_spark_tensorflow_cluster:
	ansible-playbook -i deployment/hosts deployment/emr_spark_tensorflow.yml -t launch

# Before run this command, make sure the emr-spark-tensorflow IP in deployment/hosts is correct
setup_emr_spark_tensorflow_cluster:
	ansible-playbook -i deployment/hosts deployment/emr_spark_tensorflow.yml -t setup

# Establish a tunnel for Spark history
connect_emr_spark_tensorflow_spark_history:
	scripts/connect_emr_spark_tensorflow.sh 18080

# Establish a tunnel for Jupyter notebook
connect_emr_spark_tensorflow_jupyter_notebook:
	scripts/connect_emr_spark_tensorflow.sh 8888
