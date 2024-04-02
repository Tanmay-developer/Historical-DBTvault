










-- Generated by dbtvault.

WITH row_rank_1 AS (
    SELECT * FROM (
    SELECT rr.`hk_cli_cd`, rr.`cli_id`, rr.`ld_dt_tm`, rr.`rcrd_src_nm`,
           ROW_NUMBER() OVER(
               PARTITION BY rr.`hk_cli_cd`
               ORDER BY rr.`ld_dt_tm`
           ) AS row_number
    FROM ndb.v_stg_h_cli AS rr
    WHERE rr.`hk_cli_cd` IS NOT NULL
     ) WHERE row_number = 1
),

records_to_insert AS (
    SELECT a.`hk_cli_cd`, a.`cli_id`, a.`ld_dt_tm`, a.`rcrd_src_nm`
    FROM row_rank_1 AS a
    LEFT JOIN ndb.h_cli AS d
    ON a.`hk_cli_cd` = d.`hk_cli_cd`
    WHERE d.`hk_cli_cd` IS NULL
)

SELECT * FROM records_to_insert