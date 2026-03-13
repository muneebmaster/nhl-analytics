{{ config(materialized='table') }}

select
    dateadd('day', seq4(), '2007-10-01'::date)::date as date_day
from table(generator(rowcount => 7000))
order by 1
