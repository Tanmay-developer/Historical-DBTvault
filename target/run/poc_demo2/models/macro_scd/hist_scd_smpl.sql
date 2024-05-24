
    -- back compat for old kwarg name
  
  
  
      
          
          
      
  

  

  merge into kdb.hist_scd_smpl as DBT_INTERNAL_DEST
      using hist_scd_smpl__dbt_tmp as DBT_INTERNAL_SOURCE
      on 
              DBT_INTERNAL_SOURCE.src_pk,EFFECTIVE_FROM = DBT_INTERNAL_DEST.src_pk,EFFECTIVE_FROM
          

      when matched then update set
         * 

      when not matched then insert *
