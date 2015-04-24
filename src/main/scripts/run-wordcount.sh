#!/usr/bin/env bash

SCRIPT_DIRECTORY=$(dirname $(readlink -f $0))
source ${SCRIPT_DIRECTORY}/setup.sh

WORDCOUNT=${LOCAL_HDFS}/wordcount
mkdir -p ${WORDCOUNT}

WORDCOUNT_OUTPUT=${WORDCOUNT}/output
if [ -d ${WORDCOUNT_OUTPUT} ]; then
    rm -r ${WORDCOUNT_OUTPUT}
fi

spark-submit --class interretis.sparktraining.Wordcount \
    ${PROJECT_ROOT}/target/*.jar \
    ${PROJECT_ROOT}/src/main/resources/words.txt \
    ${WORDCOUNT_OUTPUT}