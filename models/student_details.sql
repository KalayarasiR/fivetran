{{config(
    materialized='insert_mat',
    src_pk='StudentID',
    source_database="POC_TARGET",
    source_schema='SNOWFLAKE_DB_STUDENT',
    source_table='STUDENT_DETAILS'
)}}

SELECT
    StudentID,
    FirstName,
    LastName,
    DateOfBirth,
    Gender,
    Address,
    City,
    State,
    Country,
    Email,
    Load_dts
FROM {{ source('stage', 'STUDENT_DETAILS') }}
