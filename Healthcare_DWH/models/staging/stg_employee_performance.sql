{{ config(materialized='view', schema='staging') }}

SELECT
    CAST(emp_id AS VARCHAR(20)) AS emp_id,
    CAST(age AS INTEGER) AS employee_age,
    CAST(dept AS VARCHAR(50)) AS department,
    CAST(education AS VARCHAR(10)) AS education,
    CAST(recruitment_type AS VARCHAR(50)) AS recruitment_type,
    CAST(job_level AS INTEGER) AS job_level,
    CAST(rating AS INTEGER) AS performance_rating,
    CAST(awards AS INTEGER) AS awards_received,
    CAST(salary AS DECIMAL(10,2)) AS monthly_salary,
    CAST(shift AS VARCHAR(20)) AS shift,
    CURRENT_TIMESTAMP() AS loaded_at
FROM {{ source('raw', 'dim_employee') }}