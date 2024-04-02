
  
    
        create or replace table ndb.delta_lake
      
      
    using delta
      
      partitioned by (category)
      
      
      
      as
      

select *
from ndb.ntable
  