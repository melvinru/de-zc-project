{{ config(materialized='view') }}

-- with cte as (
--     select *, (TMAX)/10 as TMAXCELSIUS, (TMIN)/10 as TMINCELSIUS, (TAVG)/10 as TAVGCELSIUS from {{ source('staging','climate') }}
--     where PRCP is not null
--     order by DATE
-- )
-- select max(TMAXCELSIUS), min(TMINCELSIUS) from cte

WITH daily_temperature AS (
    SELECT
        DATE,
        NAME,
        TMAX / 10.0 AS max_temperature,
        TMIN / 10.0 AS min_temperature,
        TAVG / 10.0 AS avg_temperature
    FROM
        {{ source('staging','climate') }}
)

SELECT
    DATE,
    NAME,
    max_temperature,
    min_temperature,
    avg_temperature,
    ROUND((max_temperature - min_temperature), 2) AS temperature_range,
    ROUND(AVG(avg_temperature) OVER (ORDER BY DATE ROWS BETWEEN 364 PRECEDING AND CURRENT ROW), 2) AS rolling_yearly_avg_temperature
FROM
    daily_temperature
ORDER BY
    DATE