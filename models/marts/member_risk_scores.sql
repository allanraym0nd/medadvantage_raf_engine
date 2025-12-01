{{ config(
    materialized='table'
) }}

with members as (
    select * from {{ ref('core_members') }}
),

medical_risk as (
    select 
         member_id,
         service_year as year,
         sum(hcc_weight) as total_hcc_weight,
         count(distinct hcc_code) as hcc_count,
         string_agg(distinct hcc_code, ', ') as hcc_list
    from {{ref('member_hccs')}}
    group by member_id,service_year

),

pharmacy_risk as (
    select
        member_id,
        fill_year as year,
        sum(rxhcc_weight) as total_rxhcc_weight,
        count(distinct rxhcc_code) as rxhcc_count,
        string_agg(distinct rxhcc_code, ', ' order by rxhcc_code) as rxhcc_list
    from {{ ref('member_rxhccs') }}
    group by member_id, fill_year
),

all_years as (
    select distinct member_id, year from medical_risk
    union
    select distinct member_id, year from pharmacy_risk

),

final as (
    select 
        ay.member_id,
        ay.year,

        m.first_name,
        m.last_name,
        m.age,
        m.gender,
        m.age_band,
        m.age_risk_category,
        m.plan_type,
        m.is_active,
        
        coalesce(mr.total_hcc_weight, 0) as medical_risk_score,
        coalesce(mr.hcc_count, 0) as condition_count,
        mr.hcc_list,

        coalesce(pr.total_rxhcc_weight, 0) as pharmacy_risk_score,
        coalesce(pr.rxhcc_count, 0) as medication_count,
        pr.rxhcc_list,

        -- FINAL RAF SCORE CALCULATION

        1.0 + 
        coalesce(mr.total_hcc_weight, 0) + 
        coalesce(pr.total_rxhcc_weight, 0) as risk_adjustment_factor,

        -- Alternative calculation

        coalesce(mr.total_hcc_weight, 0) + 
        coalesce(pr.total_rxhcc_weight, 0) as total_risk_score,

        case 
            when coalesce(mr.total_hcc_weight, 0) + coalesce(pr.total_rxhcc_weight, 0) = 0
                 then 'No Chronic Conditions'
            when coalesce(mr.total_hcc_weight, 0) + coalesce(pr.total_rxhcc_weight, 0) < 0.5 
                 then 'Low Risk'
            when coalesce(mr.total_hcc_weight, 0) + coalesce(pr.total_rxhcc_weight, 0) < 1.0 
                 then 'Moderate Risk'
            when coalesce(mr.total_hcc_weight, 0) + coalesce(pr.total_rxhcc_weight, 0) < 2.0 
                 then 'High Risk'
            else 'Very High Risk'
        end as risk_category

        from all_years ay
        left join members m on ay.member_id = m.member_id
        left join medical_risk mr on ay.member_id = mr.member_id and ay.year = mr.year
        left join pharmacy_risk pr on ay.member_id = pr.member_id and ay.year = pr.year
)

select * from final
order by risk_adjustment_factor desc
