{{ 
    config(materialized='table') 
}}

select
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
    -- extract(year from date) year,
    -- extract(month from date) month			

from 
    {{
        source('staging', 'xetra_stocks') 
    }}
