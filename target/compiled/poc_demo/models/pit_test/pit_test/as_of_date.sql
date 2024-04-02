WITH as_of_date AS (
    select hsh_ky_cli_cd,ts from ndb.v_stg_s_address
    union
    select hsh_ky_cli_cd,ts from ndb.v_stg_s_name
)

 

SELECT distinct hsh_ky_cli_cd as hk_cli_cd, ts  as AS_OF_DATE FROM as_of_date