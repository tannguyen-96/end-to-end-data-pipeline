{% macro base64_to_number(column, scale=0) %}
{#
    Decode a Debezium `numeric` column.

    Debezium emits numeric as base64 of the two's-complement big-endian bytes, e.g.
    'ImA=' -> 0x2260 -> 8800 -> /100 -> 88.00. This mirrors the CASE/TO_BINARY(... 'BASE64')
    logic that used to be inlined in the Airflow merge SQL (temp/RAW.*.sql).

    Args:
        column: the base64 STRING column expression.
        scale:  divisor (10 for one decimal place, 100 for two, 0 to leave unscaled).
#}
    case
        when {{ column }} is null then null
        when left(to_varchar(to_binary({{ column }}, 'base64'), 'hex'), 1) between '8' and 'F'
            then -(to_number(repeat('F', length(to_varchar(to_binary({{ column }}, 'base64'), 'hex'))), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX')
                  - to_number(to_varchar(to_binary({{ column }}, 'base64'), 'hex'), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX') + 1)
        else to_number(to_varchar(to_binary({{ column }}, 'base64'), 'hex'), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX')
    end{% if scale %} / {{ scale }}{% endif %}
{% endmacro %}
