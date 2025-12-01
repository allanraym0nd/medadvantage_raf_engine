-- models/staging/stg_pharmacy_claims.sql

{{ config(
    materialized='view'
) }}

with source as (
    select * from {{ source('raw', 'pharmacy_claims') }}
),

cleaned as (
    select
        claim_id,
        member_id,
        fill_date,
        extract(year from fill_date) as fill_year,
        extract(month from fill_date) as fill_month,
        upper(trim(ndc_code)) as ndc_code,
        upper(trim(drug_name)) as drug_name,
        quantity,
        days_supply,
        cost
        
    from source
    where 
        member_id is not null
        and fill_date is not null
        and ndc_code is not null
        and cost > 0
)

select * from cleaned