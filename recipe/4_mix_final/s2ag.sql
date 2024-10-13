UNLOAD (
    WITH filtered_corpus AS (
        SELECT
            id,
            source,
            added,
            created,
            metadata,
            json_parse(
                regexp_replace(
                    regexp_replace(
                        regexp_replace(
                            -- remove all double quotes
                            replace(metadata.external_ids, '"', ''),
                            -- parse values and put them in the right structure
                            '{\s*''source''\s*:\s*''(.*?)''\s*,\s*''id''\s*:\s*''?(.*?)''?}\s*',
                            '{"source":"$1","id":"$2"}'
                        ),
                        -- if there are multiple entries, put them in the right structure
                        '"}(.|\s)*?{"',
                        '"},{"'
                    ),
                    '"}([^"]|\s)+]$',
                    '"}]'
                )
            ) as metadata_external_ids,
            cast(id AS INT) as corpusid,
            (metadata.title || CHR(10) || CHR(10) || metadata.abstract) AS text,
            IF(
                metadata.year < 2024
                OR (
                    metadata.year = 2024 AND
                    date(from_iso8601_timestamp(created)) < date('2024-08-01')
                ),
                'train',
                'valid'
            ) AS split
        FROM (
            SELECT
                *,
                ARRAY_MAX(
                    TRANSFORM (
                        regexp_extract_all(metadata.abstract, '\b([A-Za-z]\s)([a-z]\s)*[A-Za-z]\b'),
                        x -> length(x)
                    ) || 0
                ) AS max_single_letter_sequence,
                FILTER(
                    metadata.sources,
                    x -> NOT REGEXP_LIKE(
                        x,
                        '^Unpaywall|MergedPDFExtraction|ScienceParseMerged|Anansi|ScienceParsePlus|Adhoc|ScienceParse|Crawler|MAG$'
                    )
                ) AS filtered_sources
            FROM "temp_lucas"."llm_s2ag_v0-fos-license-2024-10-06"
            WHERE
                (metadata.title_language = 'en' OR metadata.title_perplexity > -20)
                AND metadata.abstract_language = 'en'
                AND metadata.abstract_perplexity > -20
                AND metadata.title_count >= 3
                AND metadata.abstract_count >= 50
                AND metadata.abstract_count <= 1000
                AND metadata.year >= 1970
                AND (
                    REGEXP_LIKE(
                        metadata.top_frequencies[1].token,
                        '^[A-Za-z][A-Za-z]+$'
                    )
                    OR (
                        metadata.top_frequencies[1].token = 'a'
                        AND REGEXP_LIKE(
                            metadata.top_frequencies[2].token,
                            '^[A-Za-z][A-Za-z]+$'
                        )
                    )
                )
        )
        WHERE (
            (
                CARDINALITY(filtered_sources) > 0 AND
                max_single_letter_sequence < 4
            ) OR (
                max_single_letter_sequence > 0 AND
                CARDINALITY(filtered_sources) = 0
            )
        )
    ),
    filtered_espresso AS (
        SELECT DISTINCT
            pq.corpusid,
            COALESCE(pq.s2FieldsOfStudy, ARRAY[]) as s2FieldsOfStudy,
            COALESCE(pq.fieldsOfStudy, ARRAY[]) as fieldsOfStudy
        from espresso.pq_paper as pq
        RIGHT JOIN filtered_corpus as cr
            ON pq.corpusid = cr.corpusid
    ),
    filtered_corpus_with_fos AS (
        SELECT
            cr.id,
            cr.source,
            cr.added,
            cr.created,
            cr.text,
            cr.split,
            CAST(
                ROW(
                    cr.metadata.year,
                    cr.metadata.title,
                    cr.metadata.abstract,
                    cr.metadata.sha1,
                    cr.metadata.sources,
                    pq.s2FieldsOfStudy,
                    pq.fieldsOfStudy,
                    cr.metadata_external_ids
                )
                AS
                ROW(
                    year BIGINT,
                    title VARCHAR,
                    abstract VARCHAR,
                    sha1 VARCHAR,
                    sources ARRAY<VARCHAR>,
                    s2FieldsOfStudy ARRAY<VARCHAR>,
                    extFieldsOfStudy ARRAY<VARCHAR>,
                    external_ids ARRAY<ROW(source VARCHAR, id VARCHAR)>
                )
            ) AS metadata
        FROM filtered_corpus as cr
        INNER JOIN filtered_espresso as pq
            ON pq.corpusid = cr.corpusid
    )
    SELECT
        id,
        ARBITRARY(source) AS source,
        'v3-fos-license' AS version,
        ARBITRARY(added) AS added,
        ARBITRARY(created) AS created,
        ARBITRARY(text) AS text,
        ARBITRARY(metadata) AS metadata,
        ARBITRARY(split) AS split
    FROM filtered_corpus_with_fos
    GROUP BY id
)
TO 's3://ai2-llm/pretraining-data/sources/s2/v3-fos-license/documents/2024-10-06/dataset=s2ag'
WITH (
    format='JSON',
    compression='ZSTD',
    partitioned_by = ARRAY['split']
)
