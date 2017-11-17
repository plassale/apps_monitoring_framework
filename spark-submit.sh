spark-submit \
	--master yarn-cluster \
	--num-executors 4 \
	--executor-cores 2 \
	--executor-memory 4G \
	--driver-memory 2G \
	--files main_call-restApi-Yarn.py