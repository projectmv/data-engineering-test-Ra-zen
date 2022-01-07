#!/bin/bash

# SCRIPT=$(readlink -f "$1")
# SCRIPTPATH=$(dirname "$SCRIPT")

SPARK_MASTER_NAME=${SPARK_MASTER_NAME}
SPARK_MASTER_PORT=8084
export SPARK_MASTER_URL=spark://${SPARK_MASTER_NAME}:${SPARK_MASTER_PORT}

#  =================================== module-consolidate ====================================

# MODULE=$1
MODULE="src/main/resources/"
# PATH_JARS=${MODULE}"/jars/"
PATH_JARS="target/scala-2.12/"
PATH_CONF=${MODULE}"/"

SPARK_APPLICATION=${PATH_JARS}"spark-excell-config-assembly-0.1.jar"
APPLICATION_MAIN_CLASS=anp.Execute

APPLICATION_ARGS="anp ws"
# APPLICATION_ARGS="anp DPCache_m3_4"
CONFIG=${PATH_CONF}"application.conf"

#  ==================================== spark-submit =========================================

# ${SPARK_SUBMIT_ARGS} \

spark-submit \
    --class ${APPLICATION_MAIN_CLASS} \
    --master ${SPARK_MASTER_URL} \
    --driver-java-options "-XX:+UseCompressedOops -Dconfig.file="${CONFIG} \
    ${SPARK_APPLICATION} ${APPLICATION_ARGS}


#spark-submit \
#  --class org.apache.spark.examples.SparkPi \
#  --master spark://srv-based.riachuelo.net:8084 \
#  --executor-memory 2G \
#  --total-executor-cores 4 \
#  $SPARK_HOME/examples/jars/spark-examples_2.12-3.2.0.jar
