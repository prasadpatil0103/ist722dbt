{{ config(materialized='table', schema='facts') }}

WITH staging AS (
    SELECT * FROM {{ ref('stg_employee_performance') }}
),

dates AS (
    SELECT date_key, date
    FROM {{ source('raw', 'dim_date') }}
)

SELECT
    ROW_NUMBER() OVER (ORDER BY s.emp_id) AS performance_fact_key,
    s.emp_id,
    d.date_key AS snapshot_date_key,
    s.department,
    s.shift,
    s.education,
    s.recruitment_type,
    s.monthly_salary,
    s.employee_age,
    s.job_level,
    s.performance_rating,
    s.awards_received,
    d.date AS snapshot_date,
    'hospital_employee' AS record_source,
    CURRENT_TIMESTAMP() AS created_at,
    s.loaded_at
FROM staging s
CROSS JOIN (
    SELECT date_key, date 
    FROM dates 
    WHERE date = CURRENT_DATE()
) d