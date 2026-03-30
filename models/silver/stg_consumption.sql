{{ config(materialized='view') }}

with source as (

    select * 
    from {{ source('bronze', 'raw_octopus_consumption') }}

),

deduped as (

    select *,
        row_number() over (
            partition by interval_start_utc
            order by ingestion_ts desc
        ) as rn
    from source

)

select
    interval_start_utc,
    interval_start_uk,
    kwh

from deduped
where rn = 1