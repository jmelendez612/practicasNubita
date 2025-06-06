{% macro limpiar_y_citar(texto) -%}
  "{{ texto
    | replace('\n', ' ')
    | replace('\r', ' ')
    | replace('\t', ' ')
    | replace('  ', ' ')
    | trim }}"
{%- endmacro %}

{% macro descomponer_formula(concepto, tabla_relaciones, visitados=[]) %}

  {# Obtener relaciones de la tabla #}
  {% set sql = "select agrupacion_calculada, tipo_calculo, concepto_parte, tipo_parte from " ~ tabla_relaciones %}  
  {% set results = run_query(sql) %}

  {# Convertir resultados a lista de diccionarios #}
  {% set relaciones = [] %}
  {% if results %}
    {% for row in results.rows %}
      {% set fila = {
        "agrupacion_calculada": row['AGRUPACION_CALCULADA'],
        "tipo_calculo": row['TIPO_CALCULO'],
        "concepto_parte": row['CONCEPTO_PARTE'],
        "tipo_parte": row['TIPO_PARTE']
      } %}
      {% do relaciones.append(fila) %}
    {% endfor %}
  {% endif %}

  {# Evitar ciclos #}
  {% if concepto in visitados %}
    "{{ limpiar_y_citar(concepto) }}"
  {% else %}
    {% set visitados = visitados + [concepto] %}
    {% set hijos = relaciones | selectattr("agrupacion_calculada", "equalto", concepto) | list %}

    {% if hijos | length == 0 %}
      "{{ limpiar_y_citar(concepto) }}"
    {% else %}
      {% set tipo_calculo = hijos[0]['tipo_calculo'] %}
      {% set bloque_suma = [] %}
      {% set bloque_resta = [] %}

      {% for hijo in hijos %}
        {% set subexpr = descomponer_formula(hijo.concepto_parte, tabla_relaciones, visitados) %}
        {% if hijo.tipo_parte == 'suma' %}
          {% do bloque_suma.append(subexpr) %}
        {% elif hijo.tipo_parte == 'resta' %}
          {% do bloque_resta.append(subexpr) %}
        {% endif %}
      {% endfor %}

      {# Ensamblar bloques #}
      {% if bloque_suma | length > 1 %}
        {% set bloqueA = '(' ~ bloque_suma | join(' + ') ~ ')' %}
      {% elif bloque_suma | length == 1 %}
        {% set bloqueA = bloque_suma[0] %}
      {% else %}
        {% set bloqueA = '' %}
      {% endif %}

      {% if bloque_resta | length > 1 %}
        {% set bloqueB = '(' ~ bloque_resta | join(' + ') ~ ')' %}
      {% elif bloque_resta | length == 1 %}
        {% set bloqueB = bloque_resta[0] %}
      {% else %}
        {% set bloqueB = '' %}
      {% endif %}

      {# Operador principal #}
      {% if tipo_calculo == 'suma' %}
        ({{ bloqueA }} + {{ bloqueB }})
      {% elif tipo_calculo == 'resta' %}
        ({{ bloqueA }} - {{ bloqueB }})
      {% elif tipo_calculo == 'division' %}
        ({{ bloqueA }} / {{ bloqueB }})
      {% else %}
        ({{ bloqueA }} + {{ bloqueB }}) {# fallback #}
      {% endif %}
    {% endif %}
  {% endif %}

{% endmacro %}
