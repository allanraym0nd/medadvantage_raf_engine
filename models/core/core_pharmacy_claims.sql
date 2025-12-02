
{{ config(
    materialized='table'
) }}

with claims as (
    select * from {{ ref('stg_pharmacy_claims') }}
),

rxhcc_mapping as (
    select * from {{ source('raw', 'ndc_to_rxhcc') }}
),

joined as (
    select
        c.claim_id,
        c.member_id,
        c.fill_date,
        c.fill_year,
        c.fill_month,
        c.ndc_code,
        c.drug_name,
        c.quantity,
        c.days_supply,
        c.cost,
        
        -- Add RxHCC information
        r.rxhcc_code,
        r.rxhcc_desc,
        r.weight as rxhcc_weight,
        
        -- Flag if drug maps to an RxHCC
        case 
            when r.rxhcc_code is not null then true
            else false
        end as has_rxhcc
        
    from claims c
    left join rxhcc_mapping r
        on c.ndc_code = r.ndc_code
)

select * from joined