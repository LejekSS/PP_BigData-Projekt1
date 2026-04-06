#!/bin/bash

if [ "$#" -ne 3 ]; then
    echo "Użycie: $0 <input_dir3> <input_dir4> <output_dir6>"
    exit 1
fi

INPUT_DIR3=$1
INPUT_DIR4=$2
OUTPUT_DIR6=$3
TEMP_HDFS_DIR="hdfs:///user/$(whoami)/projekt_temp_output"

# Zabezpieczenie przed błędem tworzenia katalogu w docelowym GCS
hdfs dfs -rm -r -f "$OUTPUT_DIR6"
# Usuwamy stary tymczasowy katalog HDFS
hdfs dfs -rm -r -f "$TEMP_HDFS_DIR"

# Wywołanie beeline (Zwróć uwagę, że zmieniliśmy OUTPUT_DIR6 na ten tymczasowy!)
beeline -u jdbc:hive2://localhost:10000 -n $(whoami) \
  --hivevar INPUT_DIR3="$INPUT_DIR3" \
  --hivevar INPUT_DIR4="$INPUT_DIR4" \
  --hivevar OUTPUT_DIR6="$TEMP_HDFS_DIR" \
  -f hive.hql

# Kopiowanie wyniku z superszybkiego lokalnego HDFS do Twojego GCS
echo "Kopiowanie wyniku z klastra do GCS..."
hdfs dfs -cp "$TEMP_HDFS_DIR" "$OUTPUT_DIR6"

echo "Gotowe!"