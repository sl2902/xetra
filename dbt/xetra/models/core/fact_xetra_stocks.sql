{{ 
    config(materialized='table',
    partition_by={ 
        "field": "Date",
        "data_type": "DATETIME",
        "granularity": "month"
        }) 
}}

with start_price as (
  select
    isin,
    date,
    startprice as opening_price,
    row_number() over(partition by isin, date order by time) n_r
  from
    {{
      ref('stg_xetra_stocks')
    }}
),
close_price as (
  select
    isin,
    date,
    startprice as closing_price,
    row_number() over(partition by isin, date order by time desc) n_r
  from
    {{
      ref('stg_xetra_stocks')
    }}
),
tot_traded_vol as (
  select
    isin,
    date,
    sum(tradedvolume) as traded_volume
  from
    {{
      ref('stg_xetra_stocks')
    }}
  group by
    isin,
    date
),
close_price_change as (
  select
    isin,
    date,
    lag(closing_price, 1) over(partition by isin order by date) as prev_closing_price
  from
    close_price
  where n_r = 1 
),
min_price as (
  select
    isin,
    date,
    min(startprice) as min_price
  from
    {{
      ref('stg_xetra_stocks')
    }} 
  group by
    isin,
    date
),
max_price as (
    select
    isin,
    date,
    max(startprice) as max_price
  from
    {{
      ref('stg_xetra_stocks')
    }}
  group by
    isin,
    date
)
select
 distinct
  a.isin,
  a.date,
  opening_price,
  closing_price,
  min_price,
  max_price,
  round( 100 * (1. - coalesce(prev_closing_price, closing_price)/closing_price), 2) pct_change_closing_price,
  traded_volume
from
    {{
      ref('stg_xetra_stocks')
    }}a
left join start_price b on a.isin = b.isin and a.date = b.date and b.n_r = 1
left join close_price c on b.isin = c.isin and  b.date = c.date and c.n_r = 1
left join min_price d on c.isin = d.isin and c.date = d.date
left join max_price e on d.isin = e.isin and d.date = e.date
left join close_price_change f on e.isin = f.isin and e.date = f.date
left join tot_traded_vol g on f.isin = g.isin and f.date = g.date