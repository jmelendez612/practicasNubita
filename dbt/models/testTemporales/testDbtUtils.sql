{% set tabla_relaciones = ref('stg_agrupaciones_detalladas') %}
{% set formula_cruda = descomponer_formula("BUSINESS PERFORMANCE", tabla_relaciones) %}

select
    '{{ limpiar_espacios(formula_cruda) }}' as resultado
