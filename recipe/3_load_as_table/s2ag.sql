CREATE EXTERNAL TABLE IF NOT EXISTS `temp_lucas`.`llm_s2ag_v0-fos-license-2024-10-06` (
    id STRING,
    source STRING,
    text STRING,
    version STRING,
    added STRING,
    created STRING,
    metadata STRUCT<
        year:INT,
        title:STRING,
        abstract:STRING,
        sha1:STRING,
        external_ids:ARRAY<STRUCT<source:STRING,id:STRING>>,
        sources: ARRAY<STRING>,
        title_language:STRING,
        abstract_language:STRING,
        title_perplexity:DOUBLE,
        abstract_perplexity:DOUBLE,
        title_count:INT,
        abstract_count:INT,
        top_frequencies:ARRAY<STRUCT<token:STRING,count:INT>>
    >
)
ROW FORMAT serde 'org.apache.hive.hcatalog.data.JsonSerDe'
LOCATION 's3://ai2-llm/pretraining-data/sources/s2/v0-fos-license/documents/2024-10-06/dataset=s2ag'
