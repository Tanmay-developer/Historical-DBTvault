







-- Generated by dbtvault.



WITH source_data AS (

    SELECT

    `cli_id`,
    `rcrd_src_nm`,
    `name`,
    `ts`

    FROM ndb.raw_s_name
),

derived_columns AS (

    SELECT

    `cli_id`,
    `rcrd_src_nm`,
    `name`,
    `ts`,
    `CURRENT_TIMESTAMP` AS `ld_dt_tm`,
    `ts` AS `EFFECTIVE_FROM`

    FROM source_data
),

hashed_columns AS (

    SELECT

    `CLI_ID`,
    `RCRD_SRC_NM`,
    `NAME`,
    `TS`,
    `LD_DT_TM`,
    `EFFECTIVE_FROM`,

    CAST((MD5(NULLIF(UPPER(TRIM(CAST(`cli_id` AS STRING))), ''))) AS VARCHAR(16)) AS `hsh_ky_cli_cd`,
    CAST(MD5(NULLIF(CONCAT_WS('||',
        IFNULL(NULLIF(UPPER(TRIM(CAST(`name` AS STRING))), ''), '^^')
    ), '^^')) AS VARCHAR(16)) AS `rcrd_hsh_id`

    FROM derived_columns
),

columns_to_select AS (

    SELECT

    `CLI_ID`,
    `RCRD_SRC_NM`,
    `NAME`,
    `TS`,
    `LD_DT_TM`,
    `EFFECTIVE_FROM`,
    `HSH_KY_CLI_CD`,
    `RCRD_HSH_ID`

    FROM hashed_columns
)

SELECT * FROM columns_to_select