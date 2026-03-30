{{ config(materialized='view') }}

select
    tariff_code,
    valid_from_utc,
    coalesce(valid_to_utc, TIMESTAMP '9999-12-31') as valid_to_utc,
    valid_from_uk,
    coalesce(valid_to_uk, TIMESTAMP '9999-12-31') as valid_to_uk

from {{ source('bronze', 'raw_octopus_agreements') }}