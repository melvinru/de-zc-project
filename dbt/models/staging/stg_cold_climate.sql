{{ config(materialized='view') }}

WITH daily_temperature AS (
    SELECT
        DATE,
        EXTRACT(MONTH FROM DATE) AS MONTH,
        EXTRACT(YEAR FROM DATE) AS YEAR,
        NAME,
        IFNULL(TMAX, TMIN) / 10.0 AS max_temperature,
        IFNULL(TMIN, TMAX) / 10.0 AS min_temperature,
    FROM
        {{ source('staging','climate') }}
)

SELECT
    DATE,
    NAME,
    min_temperature,
FROM
    daily_temperature
WHERE
    min_temperature is not null
    and MONTH in (1, 2)
ORDER BY
    DATE