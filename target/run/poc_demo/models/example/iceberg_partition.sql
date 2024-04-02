
  
    
        create or replace table spark_catalog.ndb.iceberg_partition
      
      
    using iceberg
      
      partitioned by (category)
      
      
      
      as
      

select *
from ndb.ntable
order by category
  