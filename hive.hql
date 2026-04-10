-- =========================
-- 0. USTAWIENIA (KRYTYCZNE)
-- =========================
SET hive.execution.engine=tez;
SET hive.vectorized.execution.enabled=true;
SET hive.auto.convert.join=true;
SET hive.exec.parallel=true;

SET hive.groupby.skewindata=true;
SET hive.exec.reducers.max=200;
SET hive.exec.reducers.bytes.per.reducer=128000000;

SET hive.map.aggr=true;
SET hive.groupby.mapaggr.checkinterval=100000;

SET tez.grouping.min-size=16777216;
SET tez.grouping.max-size=134217728;

SET hive.exec.compress.output=true;
SET mapreduce.output.fileoutputformat.compress=true;
SET mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.SnappyCodec;

-- 🔥 KLUCZOWE: WYŁĄCZENIE STATYSTYK (usuwa OOM)
SET hive.stats.autogather=false;
SET hive.compute.query.using.stats=false;
SET hive.stats.fetch.column.stats=false;
SET hive.stats.fetch.partition.stats=false;

-- =========================
-- 1. CLEAN
-- =========================
DROP TABLE IF EXISTS mr_output;
DROP TABLE IF EXISTS players_dict;
DROP TABLE IF EXISTS stage_orc;
DROP TABLE IF EXISTS result_table;

-- =========================
-- 2. INPUT
-- =========================
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

-- =========================
-- 3. STAGING (ORC)
-- =========================
CREATE TABLE stage_orc (
    region STRING,
    hero_name STRING,
    total_wins INT,
    total_draws INT,
    total_losses INT,
    total_dominant_wins INT,
    above_avg_wins BOOLEAN,
    heroes_ranking STRING   -- 🔥 lekki JSON per rekord
)
STORED AS ORC;

-- =========================
-- 4. OUTPUT JSON
-- =========================
CREATE EXTERNAL TABLE result_table (
    region STRING,
    hero_name STRING,
    total_wins INT,
    total_draws INT,
    total_losses INT,
    total_dominant_wins INT,
    above_avg_wins BOOLEAN,
    heroes_ranking STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
STORED AS TEXTFILE
LOCATION '${hivevar:OUTPUT_DIR6}';

-- =========================
-- 5. LOGIKA (BEZ OOM)
-- =========================
WITH joined_data AS (
    SELECT /*+ MAPJOIN(p) */
        p.region,
        m.hero_name,
        m.wins,
        m.draws,
        m.losses,
        m.dominant_wins
    FROM mr_output m
    JOIN players_dict p 
        ON m.player_id = p.player_id
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

hero_avg AS (
    SELECT
        hero_name,
        AVG(total_wins) AS global_avg_wins
    FROM aggregated
    GROUP BY hero_name
),

ranking AS (
    SELECT
        region,
        hero_name,
        total_wins,
        total_draws,
        total_losses,
        total_dominant_wins,
        ROW_NUMBER() OVER (PARTITION BY region ORDER BY total_wins DESC) AS rank_in_region
    FROM aggregated
)

INSERT INTO stage_orc
SELECT
    r.region,
    r.hero_name,
    r.total_wins,
    r.total_draws,
    r.total_losses,
    r.total_dominant_wins,
    (r.total_wins > h.global_avg_wins) AS above_avg_wins,
    CONCAT(
        '{"hero_name":"', r.hero_name,
        '","rank_in_region":', CAST(r.rank_in_region AS STRING),
        '}'
    ) AS heroes_ranking
FROM ranking r
JOIN hero_avg h 
    ON r.hero_name = h.hero_name;

-- =========================
-- 6. ORC → JSON
-- =========================
INSERT INTO result_table
SELECT * FROM stage_orc;
