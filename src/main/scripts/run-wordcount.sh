#!/usr/bin/env bash

THIS_DIR=$(dirname $(readlink -f $0))

source ${THIS_DIR}/setup.sh

WORDCOUNT=${LOCAL_HDFS}/wordcount
mkdir -p ${WORDCOUNT}

WORDCOUNT_OUTPUT=${WORDCOUNT}/output

if [ -d ${WORDCOUNT_OUTPUT} ]; then
    rm -r ${WORDCOUNT_OUTPUT}
fi

spark-submit --class interretis.sparktraining.Wordcount \
    ${ROOT_DIR}/target/*.jar \
    ${ROOT_DIR}/src/main/resources/words.txt \
    ${WORDCOUNT_OUTPUT}