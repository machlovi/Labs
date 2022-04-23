#!/bin/bash
../../start.sh 
source ../../env.sh
/usr/local/hadoop/bin/hdfs dfs -rm -r /part/input/
/usr/local/hadoop/bin/hdfs dfs -mkdir -p /part/input/
/usr/local/hadoop/bin/hdfs dfs -copyFromLocal ../../test-data/shot_logs.csv /part/input/
/usr/local/spark/bin/spark-submit --master=spark://$SPARK_MASTER:7077 ./part.py hdfs://$SPARK_MASTER:9000/part/input/
../../stop.sh 