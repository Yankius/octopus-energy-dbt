{{ config(materialized='view') }}

select
    tariff_code,
    cast(valid_from_utc as {{ dbt.type_timestamp() }}) as valid_from_utc,
    coalesce(
        cast(valid_to_utc as {{ dbt.type_timestamp() }}),
        {{ far_future_timestamp() }}
    ) as valid_to_utc,
    cast(valid_from_uk as {{ dbt.type_timestamp() }}) as valid_from_uk,
    coalesce(
        cast(valid_to_uk as {{ dbt.type_timestamp() }}),
        {{ far_future_timestamp() }}
    ) as valid_to_uk

from {{ source('bronze', 'raw_octopus_agreements') }}
