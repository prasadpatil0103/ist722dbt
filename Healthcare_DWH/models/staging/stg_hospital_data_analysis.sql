{{ config(materialized='view', schema='staging') }}

SELECT
    CAST(patient_id AS VARCHAR(20)) AS patient_id,
    CAST(age AS INTEGER) AS age,
    CAST(gender AS VARCHAR(10)) AS gender,
    CAST(condition AS VARCHAR(100)) AS condition,
    CAST(procedure AS VARCHAR(100)) AS procedure_name,
    CAST(cost AS DECIMAL(10,2)) AS cost,
    CAST(length_of_stay AS INTEGER) AS length_of_stay,
    CAST(readmission AS VARCHAR(3)) AS readmission_flag,
    CAST(outcome AS VARCHAR(20)) AS outcome,
    CAST(satisfaction AS INTEGER) AS satisfaction_score,
    CURRENT_TIMESTAMP() AS loaded_at
FROM {{ source('raw', 'stg_hospital_data_analysis') }}