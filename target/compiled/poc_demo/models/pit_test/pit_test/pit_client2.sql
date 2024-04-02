








-- Generated by dbtvault.


-- depends_on: ndb.v_stg_s_address
-- depends_on: ndb.v_stg_s_name


WITH as_of_dates AS (
    SELECT * FROM ndb.as_of_date
),

new_rows_as_of_dates AS (
    SELECT
        a.`hk_cli_cd`,
        b.AS_OF_DATE
    FROM ndb.h_cli AS a
    LEFT JOIN as_of_dates AS b
    ON a.`hk_cli_cd` = b.`hk_cli_cd`
),

new_rows AS (
    SELECT
        a.`hk_cli_cd`,
        a.AS_OF_DATE,COALESCE(MAX(`s_address_src`.`hsh_ky_cli_cd`), CAST('0000000000000000' AS STRING)) AS `S_ADDRESS_PK`,COALESCE(MAX(`s_address_src`.`EFFECTIVE_FROM`), CAST('1900-01-01 00:00:00.000' AS 
    timestamp
)) AS `S_ADDRESS_LDTS`,COALESCE(MAX(`s_name_src`.`hsh_ky_cli_cd`), CAST('0000000000000000' AS STRING)) AS `S_NAME_PK`,COALESCE(MAX(`s_name_src`.`EFFECTIVE_FROM`), CAST('1900-01-01 00:00:00.000' AS 
    timestamp
)) AS `S_NAME_LDTS`
    FROM new_rows_as_of_dates AS a

    LEFT JOIN ndb.s_address AS `s_address_src`
        ON a.`hk_cli_cd` = `s_address_src`.`hsh_ky_cli_cd`
        AND `s_address_src`.`EFFECTIVE_FROM` <= a.AS_OF_DATE
    LEFT JOIN ndb.s_name AS `s_name_src`
        ON a.`hk_cli_cd` = `s_name_src`.`hsh_ky_cli_cd`
        AND `s_name_src`.`EFFECTIVE_FROM` <= a.AS_OF_DATE
    GROUP BY
        a.`hk_cli_cd`, a.AS_OF_DATE
),
temp as (
    select 
    a.`hk_cli_cd`,
    a.AS_OF_DATE as start_date,
    lead(a.as_of_date) over (partition by a.`hk_cli_cd` order by a.as_of_date asc) as end_date,
    `s_address_src`.`addr`,
        `s_name_src`.`name`,
        CURRENT_TIMESTAMP as LOAD_DATETIME
    from new_rows a
    LEFT JOIN ndb.s_address AS `s_address_src`
        ON a.`hk_cli_cd` = `s_address_src`.`hsh_ky_cli_cd`
        AND `s_address_src`.`EFFECTIVE_FROM` = a.`S_ADDRESS_LDTS`
    LEFT JOIN ndb.s_name AS `s_name_src`
        ON a.`hk_cli_cd` = `s_name_src`.`hsh_ky_cli_cd`
        AND `s_name_src`.`EFFECTIVE_FROM` = a.`S_NAME_LDTS`
    ),
pit AS (
    SELECT * FROM temp
)

SELECT DISTINCT * FROM pit