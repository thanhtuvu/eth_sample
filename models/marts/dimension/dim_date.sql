{{ config(materialized='table') }}

WITH dates AS (

    SELECT
        day AS date_day
    FROM UNNEST(
        GENERATE_DATE_ARRAY(
            DATE('2026-01-01'),
            DATE('2035-12-31'),
            INTERVAL 1 DAY
        )
    ) AS day

)

SELECT *
FROM dates