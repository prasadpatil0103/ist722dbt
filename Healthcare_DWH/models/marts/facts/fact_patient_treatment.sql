{{ config(materialized='table', schema='facts') }}

WITH staging AS (
    SELECT * FROM {{ ref('stg_hospital_data_analysis') }}
),

patients AS (
    SELECT patient_id
    FROM {{ source('raw', 'dim_patient') }}
),

treatment_types AS (
    SELECT treatment_type_key, treatment_type
    FROM {{ source('raw', 'dim_treatment_type') }}
)

SELECT
    ROW_NUMBER() OVER (ORDER BY s.patient_id) AS treatment_fact_key,
    s.patient_id,
    t.treatment_type_key,
    s.condition,
    s.procedure_name,  -- Keep original (might be combined)
    s.outcome,
    s.gender,
    s.cost,
    s.length_of_stay,
    s.age AS patient_age,
    s.satisfaction_score,
    s.readmission_flag,
    'hospital_data_analysis' AS record_source,
    CURRENT_TIMESTAMP() AS created_at,
    s.loaded_at
FROM staging s
LEFT JOIN patients p ON s.patient_id = p.patient_id
LEFT JOIN treatment_types t 
    ON TRIM(t.treatment_type) = TRIM(SPLIT_PART(s.procedure_name, ' and ', 1))