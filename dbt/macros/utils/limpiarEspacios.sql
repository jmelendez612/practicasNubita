{% macro limpiar_espacios(texto) %}
  {{ texto
    | replace('\n', ' ')
    | replace('\r', ' ')
    | replace('\t', ' ')
    | replace(' ', '')
    | trim
    | replace('""', '"')
  }}
{% endmacro %}
