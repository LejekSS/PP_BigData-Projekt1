-- 0. Konfiguracja silnika
SET hive.vectorized.execution.enabled=false;
SET hive.auto.convert.join=true;
SET hive.auto.convert.join.noconditionaltask.size=30000000; 

-- 1. Zapewnienie powtarzalności skryptu - usunięcie struktur przed ich utworzeniem
DROP TABLE IF EXISTS mr_output;
DROP TABLE IF EXISTS players_dict;
DROP TABLE IF EXISTS result_table;

-- 2. Zewnętrzna tabela dla wyników z MapReduce (input_dir3)
CREATE EXTERNAL TABLE mr_output (
    player_id STRING,
    hero_name STRING,
    wins INT,
    draws INT,
    losses INT,
    dominant_wins INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '${hivevar:INPUT_DIR3}';

-- 3. Zewnętrzna tabela dla drugiego zbioru danych (input_dir4 - datasource4)
CREATE EXTERNAL TABLE players_dict (
    player_id STRING,
    nickname STRING,
    region STRING,
    team STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '${hivevar:INPUT_DIR4}'
TBLPROPERTIES ("skip.header.line.count"="1");

-- 4. Zewnętrzna tabela docelowa (Będzie się zapisywać w HDFS klastra)
CREATE EXTERNAL TABLE result_table (
    region STRING,
    hero_name STRING,
    total_wins INT,
    total_draws INT,
    total_losses INT,
    total_dominant_wins INT,
    above_avg_wins BOOLEAN,
    heroes_ranking ARRAY<STRUCT<hero_name:STRING, rank_in_region:INT>>
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
STORED AS TEXTFILE
LOCATION '${hivevar:OUTPUT_DIR6}';

-- 5. Jedno wielkie zapytanie ładujące wynik (CTE)
WITH joined_data AS (
    SELECT
        p.region,
        m.hero_name,
        m.wins,
        m.draws,
        m.losses,
        m.dominant_wins
    FROM mr_output m
    JOIN players_dict p ON m.player_id = p.player_id
),
aggregated AS (
    SELECT
        region,
        hero_name,
        SUM(wins) AS total_wins,
        SUM(draws) AS total_draws,
        SUM(losses) AS total_losses,
        SUM(dominant_wins) AS total_dominant_wins
    FROM joined_data
    GROUP BY region, hero_name
),
avg_calculated AS (
    SELECT
        region,
        hero_name,
        total_wins,
        total_draws,
        total_losses,
        total_dominant_wins,
        AVG(total_wins) OVER (PARTITION BY hero_name) AS global_avg_wins
    FROM aggregated
),
analytics AS (
    SELECT
        region,
        hero_name,
        total_wins,
        total_draws,
        total_losses,
        total_dominant_wins,
        global_avg_wins,
        RANK() OVER (PARTITION BY region ORDER BY total_wins DESC) AS rank_in_region
    FROM avg_calculated
),
region_ranking AS (
    SELECT 
        region,
        COLLECT_LIST(NAMED_STRUCT('hero_name', hero_name, 'rank_in_region', rank_in_region)) AS heroes_ranking
    FROM analytics
    GROUP BY region
)
INSERT INTO result_table
SELECT
    a.region,
    a.hero_name,
    a.total_wins,
    a.total_draws,
    a.total_losses,
    a.total_dominant_wins,
    (a.total_wins > a.global_avg_wins) AS above_avg_wins,
    r.heroes_ranking
FROM analytics a
JOIN region_ranking r ON a.region = r.region;
