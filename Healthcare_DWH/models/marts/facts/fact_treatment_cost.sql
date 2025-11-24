{{ config(materialized='table', schema='facts') }}

WITH staging AS (
    SELECT * FROM {{ ref('stg_treatment') }}
),

treatment_types AS (
    SELECT treatment_type_key, treatment_type
    FROM {{ source('raw', 'dim_treatment_type') }}
),

dates AS (
    SELECT date_key, date
    FROM {{ source('raw', 'dim_date') }}
)

SELECT
    ROW_NUMBER() OVER (ORDER BY s.treatment_id) AS cost_fact_key,
    d.date_key AS treatment_date_key,
    t.treatment_type_key,
    s.treatment_id,
    s.appointment_id,
    s.description,
    s.treatment_cost,
    'hospital_treatment' AS record_source,
    CURRENT_TIMESTAMP() AS created_at,
    s.loaded_at
FROM staging s
LEFT JOIN treatment_types t ON s.treatment_type = t.treatment_type
LEFT JOIN dates d ON s.treatment_date = d.date