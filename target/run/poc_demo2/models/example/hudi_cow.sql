
    -- back compat for old kwarg name
  
  
  
      
          
          
      
  

  

  merge into kdb.hudi_cow as DBT_INTERNAL_DEST
      using hudi_cow__dbt_tmp as DBT_INTERNAL_SOURCE
      on 
              DBT_INTERNAL_SOURCE.application_id = DBT_INTERNAL_DEST.application_id
          

      when matched then update set
         * 

--      when not matched then insert *
      when not matched then insert
         * 
