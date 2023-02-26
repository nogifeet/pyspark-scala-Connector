#!/bin/bash 

#export PYSPARK_PYTHON=/usr/lib/python2.7

#python -V

TIMESTAMP="`date +%Y-%m-%d` `date +%T%z`"

log() {
    echo "$TIMESTAMP : $@" #>> $LOGFILE_PATH
}

log "[INFO] [${BASH_SOURCE[0]}:${LINENO}] Starting jsl-submission"

input_path=$1
output_path=$2
spark_submit_path=$3
jars_path=$4

driver_mem=1G
executor_mem=2G
cores=2
executors=2

$spark_submit_path \
--jars $jars_path \
--deploy-mode client \
--master yarn \
--conf spark.yarn.maxAppAttempts=1 \
--driver-memory $driver_mem \
--executor-memory $executor_mem \
--num-executors $executors \
--executor-cores $cores \
callScalaClass.py $input_path $output_path

log "[INFO] [${BASH_SOURCE[0]}:${LINENO}] END"
