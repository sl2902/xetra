{{ 
    config(materialized='table') 
}}

with base_table as (
    select
        key,
        isin,
        mnemonic,		
        securitydesc,				 
        securitytype,				
        currency,				
        securityID,				
        date,			
        time,			
        startprice,			
        maxprice,				
        minprice,				
        endprice,				
        tradedvolume,
        row_number() over(partition by key order by date, time) r_n
        -- extract(year from date) year,
        -- extract(month from date) month			

    from 
        {{
            source('staging', 'xetra_stocks') 
        }}
)
select 
    key,
    isin,
    mnemonic,		
    securitydesc,				 
    securitytype,				
    currency,				
    securityID,				
    date,			
    time,			
    startprice,			
    maxprice,				
    minprice,				
    endprice,				
    tradedvolume
from 
    base_table
where 
    r_n = 1
