create or replace view ndb.raw_h_cli
  
  as
    with cli AS (
SELECT distinct trim(cli.cli_id) as cli_id,
'dummy' AS rcrd_src_nm
FROM ndb.cli_hub cli
WHERE 1=1
)

SELECT
cli.cli_id,
cli.rcrd_src_nm
FROM cli
