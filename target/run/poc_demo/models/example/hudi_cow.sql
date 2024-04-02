
  
    
        create table ndb.hudi_cow
      
      
    using hudi
      options (type "cow" , primaryKey "application_id" , preCombineField "watermark" 
        )
      partitioned by (category)
      
      
      
      as
      

select *
from ndb.ntable
  