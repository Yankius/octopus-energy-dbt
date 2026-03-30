{{ config(
    materialized='incremental',
    unique_key='interval_start_utc'
) }}

with consumption as (

    select * from {{ ref('stg_consumption') }}

    {% if is_incremental() %}
        where interval_start_utc >= (
            select max(interval_start_utc) - interval '1 day'
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

-- Step 1: map consumption → active tariff agreement
consumption_with_agreement as (

    select
        c.*,
        a.tariff_code

    from consumption c
    left join agreements a
        on c.interval_start_utc >= a.valid_from_utc
        and c.interval_start_utc < a.valid_to_utc

),

-- Step 2: join tariff rates
final as (

    select
        c.interval_start_utc,
        c.interval_start_uk,

        c.kwh,
        c.tariff_code,
        t.unit_rate,

        -- 💰 CORE METRIC
        c.kwh * t.unit_rate as cost,

        -- useful analytics fields
        date_trunc('day', c.interval_start_uk) as usage_date,
        extract(hour from c.interval_start_uk) as hour_of_day,

        case 
            when extract(hour from c.interval_start_uk) between 0 and 6 then 'night'
            when extract(hour from c.interval_start_uk) between 16 and 19 then 'peak'
            else 'day'
        end as time_band

    from consumption_with_agreement c

    left join tariffs t
        on c.tariff_code = t.tariff_code
        and c.interval_start_utc >= t.valid_from_utc
        and c.interval_start_utc < t.valid_to_utc

)

select * from final