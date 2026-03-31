{{ config(
    materialized='incremental',
    unique_key='interval_start_utc'
) }}

with consumption as (
    select * from {{ ref('stg_consumption') }}
    {% if is_incremental() %}
        -- We cast the result of the entire dateadd macro to TIMESTAMP
        where cast(interval_start_utc as timestamp) >= (
            select cast(
                {{ dbt.dateadd('day', -1, "max(interval_start_utc)") }}
            as timestamp)
            from {{ this }}
        )
    {% endif %}
),

agreements as (
    select * from {{ ref('stg_agreements') }}
),

tariffs as (
    select * from {{ ref('stg_tariffs') }}
),

final as (

    select
        c.interval_start_utc,
        c.interval_start_uk,
        c.kwh,
        a.tariff_code,
        t.unit_rate,

        -- 💰 CORE METRIC
        c.kwh * t.unit_rate as cost,

        {{ dbt.date_trunc('day', 'c.interval_start_uk') }} as usage_date,
        extract(hour from c.interval_start_uk) as hour_of_day,

        case 
            when extract(hour from c.interval_start_uk) between 0 and 6 then 'night'
            when extract(hour from c.interval_start_uk) between 16 and 19 then 'peak'
            else 'day'
        end as time_band

    from consumption c

    left join agreements a
        on cast(c.interval_start_utc as timestamp) >= cast(a.valid_from_utc as timestamp)
        and cast(c.interval_start_utc as timestamp) < cast(a.valid_to_utc as timestamp)

    left join tariffs t
        on a.tariff_code = t.tariff_code
        and cast(c.interval_start_utc as timestamp) >= cast(t.valid_from_utc as timestamp)
        and cast(c.interval_start_utc as timestamp) < cast(t.valid_to_utc as timestamp)

)

select * from final