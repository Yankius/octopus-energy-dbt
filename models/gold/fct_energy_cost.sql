{{ config(
    materialized='incremental',
    unique_key='interval_start_utc'
) }}

with consumption as (
    select * from {{ ref('stg_consumption') }}
    {% if is_incremental() %}
        where interval_start_utc >= (
            select {{ dbt.dateadd('day', -1, "max(interval_start_utc)") }}
            from {{ this }}
        )
    {% endif %}
),

agreements as (
    select * from {{ ref('stg_agreements') }}
),

tariffs as (
    select * from {{ ref('stg_tariffs') }}
), -- <--- THIS COMMA IS ESSENTIAL

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
        on c.interval_start_utc >= a.valid_from_utc
        and c.interval_start_utc < a.valid_to_utc

    left join tariffs t
        on a.tariff_code = t.tariff_code
        and c.interval_start_utc >= t.valid_from_utc
        and c.interval_start_utc < t.valid_to_utc

)

select * from final
