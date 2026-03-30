/*
  This test ensures there are no gaps in the 30-minute consumption data.
  It generates a complete series of timestamps between the min and max 
  found in the fact table and checks if any are missing.
*/

with series as (

    select interval_start_utc
    from unnest(
        generate_timestamp_array(
            (select min(interval_start_utc) from {{ ref('fct_energy_cost') }}),
            (select max(interval_start_utc) from {{ ref('fct_energy_cost') }}),
            interval 30 minute
        )
    ) as interval_start_utc

),

missing as (

    select 
        s.interval_start_utc
    from series s
    left join {{ ref('fct_energy_cost') }} f
        on s.interval_start_utc = f.interval_start_utc
    where f.interval_start_utc is null

)

select * from missing