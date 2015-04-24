#!/usr/bin/env bash

THIS_DIR=$(dirname $(readlink -f $0))
ROOT_DIR=$(dirname $(readlink -f ${THIS_DIR}/../..))

export LOCAL_HDFS=${ROOT_DIR}/target/hdfs/
mkdir -p ${LOCAL_HDFS}

