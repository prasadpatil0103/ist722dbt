{{ config(materialized='view', schema='staging') }}

SELECT
    CAST(treatment_id AS VARCHAR(20)) AS treatment_id,
    CAST(appointment_id AS VARCHAR(20)) AS appointment_id,
    CAST(treatment_type AS VARCHAR(50)) AS treatment_type,
    CAST(description AS VARCHAR(200)) AS description,
    CAST(cost AS DECIMAL(10,2)) AS treatment_cost,
    CAST(treatment_date AS DATE) AS treatment_date,
    CURRENT_TIMESTAMP() AS loaded_at
FROM {{ source('raw', 'stg_treatment') }}