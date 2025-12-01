{{config(
    materialized='table'
)}}

with members as (
    select * from {{ref('stg_members')}}
),

enriched as (
    select
        member_id,
        first_name,
        last_name,
        date_of_birth,
        age,
        gender,
        enrollment_start_date,
        enrollment_end_date,
        plan_type,
        is_active,

        case 
            when age < 35 then '0-34'
            when age between 35 and 44 then '35-44'
            when age between 45 and 54 then '45-54'
            when age between 55 and 64 then '55-64'
            when age between 65 and 74 then '65-74'
            when age >= 75 then '75+'
            else 'Unknown'
        end as age_band,

        case  
            when age >= 65 then 'Medicare Eligible'
            when age >= 55 then 'Pre-Medicare'
            else 'Under 55'
        end as age_risk_category

    from members
)

select * from enriched