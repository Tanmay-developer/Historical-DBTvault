



WITH as_of_dates AS (
  
    
    select cstmr_id as src_pk, ldts as EFFECTIVE_FROM from kdb.customer_addr
        
            where ldts > cast('2024-05-25 14:20:00.0' as timestamp)
        union
        select cstmr_id as src_pk, max(ldts) as EFFECTIVE_FROM from kdb.customer_addr group by cstmr_id
    union
    
  
    
    select cstmr_id as src_pk, ldts as EFFECTIVE_FROM from kdb.customer_phn
        
            where ldts > cast('2024-05-25 14:20:00.0' as timestamp)
        union
        select cstmr_id as src_pk, max(ldts) as EFFECTIVE_FROM from kdb.customer_phn group by cstmr_id
  
),


historical_data as (
    SELECT 
    a.src_pk,
    a.EFFECTIVE_FROM,
    COALESCE(
                customer_addr_src.addr,
                LEAD(customer_addr_src.addr) IGNORE NULLS OVER (PARTITION BY a.src_pk ORDER BY a.EFFECTIVE_FROM DESC)
            ) AS addr,
        COALESCE(
                customer_phn_src.phone_no,
                LEAD(customer_phn_src.phone_no) IGNORE NULLS OVER (PARTITION BY a.src_pk ORDER BY a.EFFECTIVE_FROM DESC)
            ) AS phone_no,
        CURRENT_TIMESTAMP as LOAD_DATETIME
    from as_of_dates a
    LEFT JOIN kdb.customer_addr AS customer_addr_src
        ON a.src_pk = customer_addr_src.cstmr_id
        AND a.EFFECTIVE_FROM = customer_addr_src.ldts 
    LEFT JOIN kdb.customer_phn AS customer_phn_src
        ON a.src_pk = customer_phn_src.cstmr_id
        AND a.EFFECTIVE_FROM = customer_phn_src.ldts 
    )


select * from historical_data