create or replace view ndb.final_table
  
  as
    select 
hk_cli_cd,
start_date,
end_date,
addr,
name
from (
select distinct p.hk_cli_cd, p.as_of_date as start_date,
lead(p.as_of_date) over (partition by p.hk_cli_cd order by p.as_of_date asc) as end_date,
a.addr, b.name
from ndb.pit_client p
LEFT JOIN ndb.s_address a
    ON p.s_address_pk = a.hsh_ky_cli_cd and p.s_address_ldts = a.effective_from
LEFT JOIN ndb.s_name b
    ON p.s_name_ldts = b.effective_from and p.hk_cli_cd = b.hsh_ky_cli_cd
order by 1 asc
)
