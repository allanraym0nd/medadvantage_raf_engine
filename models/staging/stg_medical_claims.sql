{{ config(
    materialized='view'
) }}

with source as (
    select * from {{ source('raw', 'medical_claims') }}
),

cleaned as (
    select
        claim_id,
        member_id,
        service_date,
        extract(year from service_date) as service_year,
        extract(month from service_date) as service_month,
        upper(trim(diagnosis_code)) as diagnosis_code,
        provider_id,
        claim_amount

        from source 
        where
                member_id is not null
                and service_date is not null
                and diagnosis_code is not null
                and claim_amount > 0           

)

select * from cleaned