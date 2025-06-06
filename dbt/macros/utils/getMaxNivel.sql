{% macro get_max_nivel() %}
    {% set query %}
        select max(nivel) as max_nivel from {{ ref('inter_agrupaciones_niveladas') }}
    {% endset %}

    {% set results = run_query(query) %}
    {% if execute %}
        {% set nivel_max = results.columns[0].values()[0] %}
        {{ return(nivel_max) }}
    {% else %}
        {{ return(0) }}
    {% endif %}
{% endmacro %}