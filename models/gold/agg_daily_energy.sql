{{ config(materialized='table') }}

select
    usage_date,
    sum(kwh) as total_kwh,
    sum(cost) as total_cost,
    avg(unit_rate) as avg_rate

from {{ ref('fct_energy_cost') }}

group by 1