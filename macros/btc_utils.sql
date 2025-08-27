{% macro convert_to_usd(column_name) %}

    {{ column_name }} * (
        select (price)
        from {{ ref('btc_usd_max') }}
        where to_date(replace(snapped_at,' UTC','')) = to_date('2025-08-26','yyyy-mm-dd')
    )

{% endmacro %}
