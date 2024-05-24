









WITH as_of_date AS (
  
    
    select cstmr_id as cstmr_id, ldts as EFFECTIVE_FROM from kdb.customer_addr
        
    union
    
  
    
    select cstmr_id as cstmr_id, ldts as EFFECTIVE_FROM from kdb.customer_phn
        
  
),

as_of_dates AS (
    SELECT distinct cstmr_id, EFFECTIVE_FROM as AS_OF_DATE FROM as_of_date
),

new_rows_as_of_dates AS (
    SELECT
        a.cstmr_id,
        b.AS_OF_DATE
    FROM kdb.customer_addr AS a
    LEFT JOIN as_of_dates AS b
    ON a.cstmr_id = b.cstmr_id
),

new_rows AS (
    SELECT
        a.cstmr_id,
        a.AS_OF_DATE,COALESCE(MAX(customer_addr_src.cstmr_id), 
            CAST('0000000000000000' AS STRING)) AS CUSTOMER_ADDR_PK,COALESCE(MAX(customer_addr_src.ldts), 
        CAST('1900-01-01 00:00:00.000' AS timestamp)) AS CUSTOMER_ADDR_LDTS,COALESCE(MAX(customer_phn_src.cstmr_id), 
            CAST('0000000000000000' AS STRING)) AS CUSTOMER_PHN_PK,COALESCE(MAX(customer_phn_src.ldts), 
        CAST('1900-01-01 00:00:00.000' AS timestamp)) AS CUSTOMER_PHN_LDTS
    FROM new_rows_as_of_dates AS a

    LEFT JOIN kdb.customer_addr AS customer_addr_src
        ON a.cstmr_id = customer_addr_src.cstmr_id
        AND customer_addr_src.ldts <= a.AS_OF_DATE
    LEFT JOIN kdb.customer_phn AS customer_phn_src
        ON a.cstmr_id = customer_phn_src.cstmr_id
        AND customer_phn_src.ldts <= a.AS_OF_DATE
    GROUP BY
        a.cstmr_id, a.AS_OF_DATE
),
temp as (
    select 
    a.cstmr_id,
    a.AS_OF_DATE as start_date,
    lead(a.as_of_date) over (partition by a.cstmr_id order by a.as_of_date asc) as end_date,
    customer_addr_src.addr,
        customer_phn_src.phone_no,
        CURRENT_TIMESTAMP as LOAD_DATETIME
    from new_rows a
    LEFT JOIN kdb.customer_addr AS customer_addr_src
        ON a.cstmr_id = customer_addr_src.cstmr_id
        AND customer_addr_src.ldts = a.CUSTOMER_ADDR_LDTS
    LEFT JOIN kdb.customer_phn AS customer_phn_src
        ON a.cstmr_id = customer_phn_src.cstmr_id
        AND customer_phn_src.ldts = a.CUSTOMER_PHN_LDTS
    ),
pit AS (
    SELECT * FROM temp
)

SELECT DISTINCT * FROM pit