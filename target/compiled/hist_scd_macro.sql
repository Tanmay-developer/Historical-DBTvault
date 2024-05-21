WITH as_of_date AS (


    select etli_applicationnumber as etli_applicationnumber, psa_record_effective_from_datetime as EFFECTIVE_FROM from dataverse_preprod_mirror_d365_psa.etli_applications_psa
    union



    select pol_id as etli_applicationnumber, max(psa_record_effective_from_datetime) as EFFECTIVE_FROM from dataverse_preprod_mirror_ing_psa.tagrp_psa group by pol_id

),

as_of_dates AS (
    SELECT distinct etli_applicationnumber, EFFECTIVE_FROM as AS_OF_DATE FROM as_of_date
),

new_rows_as_of_dates AS (
    SELECT
        a.etli_applicationnumber,
        b.AS_OF_DATE
    FROM dataverse_preprod_mirror_d365_psa.etli_applications_psa AS a
    LEFT JOIN as_of_dates AS b
    ON a.etli_applicationnumber = b.etli_applicationnumber
),

new_rows AS (
    SELECT
        a.etli_applicationnumber,
        a.AS_OF_DATE,COALESCE(MAX(etli_applications_psa_src.etli_applicationnumber),
            CAST('0000000000000000' AS STRING)) AS ETLI_APPLICATIONS_PSA_PK,COALESCE(MAX(etli_applications_psa_src.psa_record_effective_from_datetime),
        CAST('1900-01-01 00:00:00.000' AS timestamp)) AS ETLI_APPLICATIONS_PSA_LDTS,COALESCE(MAX(tagrp_psa_src.pol_id),
            CAST('0000000000000000' AS STRING)) AS TAGRP_PSA_PK,COALESCE(MAX(tagrp_psa_src.psa_record_effective_from_datetime),
        CAST('1900-01-01 00:00:00.000' AS timestamp)) AS TAGRP_PSA_LDTS
    FROM new_rows_as_of_dates AS a

    LEFT JOIN dataverse_preprod_mirror_d365_psa.etli_applications_psa AS etli_applications_psa_src
        ON a.etli_applicationnumber = etli_applications_psa_src.etli_applicationnumber
        AND etli_applications_psa_src.psa_record_effective_from_datetime <= a.AS_OF_DATE
    LEFT JOIN dataverse_preprod_mirror_ing_psa.tagrp_psa AS tagrp_psa_src
        ON a.etli_applicationnumber = tagrp_psa_src.pol_id
        AND tagrp_psa_src.psa_record_effective_from_datetime <= a.AS_OF_DATE
    GROUP BY
        a.etli_applicationnumber, a.AS_OF_DATE
),
temp as (
    select
    a.etli_applicationnumber,
    a.AS_OF_DATE as start_date,
    lead(a.as_of_date) over (partition by a.etli_applicationnumber order by a.as_of_date asc) as end_date,
    etli_applications_psa_src.etli_proposername,
        etli_applications_psa_src.etli_policystatusname,
        etli_applications_psa_src.etli_branchcode,
        tagrp_psa_src.agt_id,
        CURRENT_TIMESTAMP as LOAD_DATETIME
    from new_rows a
    LEFT JOIN dataverse_preprod_mirror_d365_psa.etli_applications_psa AS etli_applications_psa_src
        ON a.etli_applicationnumber = etli_applications_psa_src.etli_applicationnumber
        AND etli_applications_psa_src.psa_record_effective_from_datetime = a.ETLI_APPLICATIONS_PSA_LDTS
    LEFT JOIN dataverse_preprod_mirror_ing_psa.tagrp_psa AS tagrp_psa_src
        ON a.etli_applicationnumber = tagrp_psa_src.pol_id
        AND tagrp_psa_src.psa_record_effective_from_datetime = a.TAGRP_PSA_LDTS
    ),
pit AS (
    SELECT * FROM temp
)

SELECT DISTINCT * FROM pit