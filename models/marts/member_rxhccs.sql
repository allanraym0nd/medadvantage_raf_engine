-- models/marts/member_rxhccs.sql

{{ config(
    materialized='table'
) }}

with pharmacy_claims as (
    select * from {{ ref('core_pharmacy_claims') }}
),

-- Get distinct RxHCCs per member per year
unique_rxhccs as (
    select
        member_id,
        fill_year,
        rxhcc_code,
        rxhcc_desc,
        max(rxhcc_weight) as rxhcc_weight
    from pharmacy_claims
    where rxhcc_code is not null
    group by 
        member_id, 
        fill_year, 
        rxhcc_code, 
        rxhcc_desc
)

select * from unique_rxhccs