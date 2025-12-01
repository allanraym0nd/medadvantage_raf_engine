{{ config(
    materialized = 'view'
)}}

with source as (
    select * from {{ source ('raw', 'members')}}
),

cleaned as (
    select 
            member_id,
            upper(trim(first_name)) as first_name,
            upper(trim(last_name)) as last_name,
            date_of_birth,
            extract(year from age(current_date, date_of_birth)) as age,
            upper(trim(gender)) as gender,
            enrollment_start_date,
            enrollment_end_date,
            upper(trim(plan_type)) as plan_type,

            -- Flag for active enrollement
            case 
                when enrollment_end_date >=current_date then true
                else false
            end as is_active 

            from source
            where member_id is not null

)

select * from cleaned 



