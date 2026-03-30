{{ config(
    materialized='incremental',
    unique_key='interval_start_utc'
) }}

with consumption as (

    select * from {{ ref('stg_consumption') }}

    {% if is_incremental() %}
        -- dbt.dateadd is universal: works in DuckDB and BigQuery
        where interval_start_utc >= (
            select {{ dbt.dateadd('day', -1, "max(interval_start_utc)") }}
            from {{ this }}
        )
    {% endif %}

),

-- ... [agreements and tariffs CTEs stay the same] ...

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

        -- dbt.date_trunc handles the 'day' vs day and order of arguments
        {{ dbt.date_trunc('day', 'c.interval_start_uk') }} as usage_date,
        
        -- extract works the same in both if we keep it simple
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