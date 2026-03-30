with series as (

    select *
    from generate_series(
        (select min(interval_start_utc) from {{ ref('fct_energy_cost') }}),
        (select max(interval_start_utc) from {{ ref('fct_energy_cost') }}),
        interval '30 minutes'
    ) as t(interval_start_utc)

),

missing as (

    select s.interval_start_utc
    from series s
    left join {{ ref('fct_energy_cost') }} f
        on s.interval_start_utc = f.interval_start_utc
    where f.interval_start_utc is null

)

select * from missing