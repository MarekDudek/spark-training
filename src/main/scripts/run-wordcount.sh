#!/usr/bin/env bash

THIS_DIR=$(dirname $(readlink -f $0))

source ${THIS_DIR}/setup.sh

WORDCOUNT=${LOCAL_HDFS}/wordcount
mkdir -p ${WORDCOUNT}

if [ -d ${WORDCOUNT}/output ]; then
    rm -r ${WORDCOUNT}/output
fi

spark-submit --class interretis.sparktraining.Wordcount \
    ./target/*.jar \
    ./src/main/resources/words.txt \
    ./target/hdfs/wordcount/output