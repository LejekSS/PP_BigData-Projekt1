#!/bin/bash

if [ "$#" -ne 3 ]; then
    echo "Uzycie: $0 <input_dir3> <input_dir4> <output_dir6>"
    exit 1
fi

INPUT_DIR3=$1
INPUT_DIR4=$2
OUTPUT_DIR6=$3

hdfs dfs -rm -r -f "$OUTPUT_DIR6"

beeline -u jdbc:hive2://localhost:10000 -n $(whoami) \
  --hivevar INPUT_DIR3="$INPUT_DIR3" \
  --hivevar INPUT_DIR4="$INPUT_DIR4" \
  --hivevar OUTPUT_DIR6="$OUTPUT_DIR6" \
  -f hive.hql
