{{config (
    materialized='table'
)}}

with claims as (
    select * from {{ref('stg_medical_claims')}}
), 

hcc_mapping as (
    select * from {{source('raw', 'icd_to_hcc')}}
),

joined as (
    select
        c.claim_id,
        c.member_id,
        c.service_date,
        c.service_year,
        c.service_month,
        c.diagnosis_code,
        c.provider_id,
        c.claim_amount,

        --Add HCC information

        h.hcc_code,
        h.hcc_desc,
        h.weight as hcc_weight,

        case    
            when h.hcc_code is not null then true
            else false

        end as has_hcc

        from claims c
        left join hcc_mapping h 
            on c.diagnosis_code = h.icd10_code

)

select * from joined