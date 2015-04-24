#!/usr/bin/env bash

SCRIPT_DIRECTORY=$(dirname $(readlink -f $0))
export PROJECT_ROOT=$(dirname $(readlink -f ${SCRIPT_DIRECTORY}/../..))

export LOCAL_HDFS=${PROJECT_ROOT}/target/hdfs/
mkdir -p ${LOCAL_HDFS}

