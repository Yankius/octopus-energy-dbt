{% macro far_future_timestamp() %}
  cast('9999-12-31 00:00:00+00:00' as {{ dbt.type_timestamp() }})
{% endmacro %}
