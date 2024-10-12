UNLOAD (
    WITH espresso_pq_metadata AS (
        SELECT
            DISTINCT pq.corpusid as id,
            pq.fieldsofstudy as fields_of_study,
            pq.id as sha1,
            to_iso8601(
                from_iso8601_timestamp(pq.earliestacquisitiondate)
            ) as added
        FROM espresso.pq_paper AS pq
        INNER JOIN s2orc_papers.latest AS s2orc
            ON pq.corpusid = s2orc.id
    ),
    content_espresso_metadata AS (
        SELECT
            ep.*,
            to_iso8601(
                CAST(
                    IF(
                        cp.pub_date IS null,
                        IF(
                            cp.year IS null,
                            date('0001-01-01'),
                            date(CAST(cp.year as VARCHAR(4)) || '-01-01')
                        ),
                        pub_date
                    )
                    AS timestamp
                )
            ) AS created
        FROM "content_ext"."papers" as cp
        INNER JOIN espresso_pq_metadata as ep
            ON ep.id = cp.corpus_paper_id
    ),
    s2orc_open_access AS (
        SELECT
            id,
            metadata.publication_date.year AS year,
            metadata.title AS title,
            metadata.abstract AS abstract,
            content.grobid.contents AS full_text,
            content.source.oa_info.license as oa_license,
            content.source.oa_info.open_access_url as oa_url,
            content.source.oa_info.status as oa_status,
            content.source.pdf_src as pdf_src,
            content.source.pdf_hash as pdf_hash,
            TRANSFORM(
                CAST(
                    JSON_PARSE(content.grobid.annotations.paragraph)
                    AS ARRAY(json)
                ),
                x -> CAST(
                    ROW(
                        JSON_EXTRACT(x, '$.start'),
                        JSON_EXTRACT(x, '$.end'),
                        'paragraph'
                    ) AS ROW(bos INTEGER, eos INTEGER, type VARCHAR)
                )
            ) AS paragraph_loc,
            TRANSFORM(
                CAST(
                    JSON_PARSE(content.grobid.annotations.section_header)
                    AS ARRAY(json)
                ),
                x -> CAST(
                    ROW(
                        JSON_EXTRACT(x, '$.start'),
                        JSON_EXTRACT(x, '$.end'),
                        'section_header'
                    ) AS ROW(bos INTEGER, eos INTEGER, type VARCHAR)
                )
            ) AS section_header_loc
        FROM "s2orc_papers"."oa_releases"
        WHERE
            year=2024 AND
            month=10 AND
            day=06 AND
            content.grobid.contents is not null
    ),
    prepared_locs AS (
        SELECT
            id,
            year,
            title,
            abstract,
            full_text,
            ARRAY_SORT(
                ARRAY_DISTINCT(paragraph_loc || section_header_loc)
            ) AS all_paralocs,
            oa_license,
            oa_url,
            oa_status,
            pdf_src,
            pdf_hash
        FROM s2orc_open_access
    ),
    extracted_paragraphs AS (
        SELECT
            id,
            year,
            title,
            abstract,
            TRANSFORM(
                all_paralocs,
                x -> CAST(
                     ROW(
                        SUBSTR(full_text, x.bos, x.eos - x.bos + 1),
                        x.type
                    ) AS ROW(text VARCHAR, type VARCHAR)
                )
            ) AS all_paragraphs,
            oa_license,
            oa_url,
            oa_status,
            pdf_src,
            pdf_hash
        FROM prepared_locs
    )
    SELECT
        pt.*,
        ep.fields_of_study,
        ep.sha1,
        ep.added,
        ep.created,
        -- make 10 partitions for smaller output files
        pt.id % 10 as part_id
    FROM extracted_paragraphs AS pt
    INNER JOIN content_espresso_metadata AS ep
        ON pt.id = ep.id
)
TO 's3://ai2-llm/pretraining-data/sources/s2/raw/2024_10_06/s2orc/'
WITH (
    format='PARQUET',
    partitioned_by = ARRAY['part_id']
)
