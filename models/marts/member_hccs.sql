{{ config(
    materialized='table'
) }}

with medical_claims as (
    select * from{{ref ('core_medical_claims')}}
),

-- Get distinct HCCs per member per year

unique_hccs as (
    select 
            member_id,
            service_year,
            hcc_code,
            hcc_desc,
            max(hcc_weight) as hcc_weight -- taking max hcc if theres somehow different weights for same HCC
    from medical_claims 
    where hcc_code is not null
    group by 
        member_id,
        service_year,
        hcc_code, 
        hcc_desc
                
)

select * from unique_hccs