{{ config(materialized='view') }}

select *, (TMAX-32)*5/9 as TMAXCELSIUS from {{ source('staging','climate') }}
order by DATE
limit 100
