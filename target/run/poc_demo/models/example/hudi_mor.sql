
  
    
        create table ndb.hudi_mor
      
      
    using hudi
      options (primaryKey "application_id" , preCombineField "watermark" , type "mor" 
        )
      partitioned by (category)
      
      
      
      as
      

select *
from ndb.ntable
  