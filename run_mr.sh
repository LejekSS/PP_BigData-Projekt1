#!/bin/bash

# 1. Sprawdzenie poprawnej liczby argumentów
if [ "$#" -ne 2 ]; then
    echo "Użycie: $0 <input_dir1> <output_dir3>"
    exit 1
fi

INPUT_DIR1=$1
OUTPUT_DIR3=$2

# Zmienna wskazująca na Twój skompilowany plik JAR. 
# Upewnij się, że nazwa pliku JAR zgadza się z tym, co masz na klastrze!
JAR_FILE="BigData_01-1.0-SNAPSHOT.jar"
MAIN_CLASS="lab2.example.EsportsHeroesStatsJob"

# 2. Zapewnienie powtarzalności - usunięcie katalogu wynikowego, jeśli istnieje
echo "Usuwanie poprzedniego katalogu wynikowego: $OUTPUT_DIR3"
hdfs dfs -rm -r -f "$OUTPUT_DIR3"

# 3. Uruchomienie zadania MapReduce
echo "Uruchamianie zadania MapReduce..."
hadoop jar "$JAR_FILE" "$MAIN_CLASS" "$INPUT_DIR1" "$OUTPUT_DIR3"

# 4. Sprawdzenie statusu wykonania
if [ $? -eq 0 ]; then
    echo "Zadanie MapReduce zakończyło się SUKCESEM!"
else
    echo "BŁĄD: Zadanie MapReduce zakończyło się niepowodzeniem."
    exit 1
fi
