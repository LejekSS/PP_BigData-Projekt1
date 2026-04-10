#!/bin/bash

if [ "$#" -ne 2 ]; then
    echo "Uzycie: $0 <input_dir1> <output_dir3>"
    exit 1
fi

INPUT_DIR1=$1
OUTPUT_DIR3=$2

JAR_FILE="BigData_01-1.0-SNAPSHOT.jar"
MAIN_CLASS="bigdata.mapreduce.main"

hdfs dfs -rm -r -f "$OUTPUT_DIR3"
hadoop jar "$JAR_FILE" "$MAIN_CLASS" "$INPUT_DIR1" "$OUTPUT_DIR3"

if [ $? -eq 0 ]; then
    echo "Przetwarzanie zakończone sukcesem"
else
    echo "Przetwarzanie zakończone bledem"
    exit 1
fi
