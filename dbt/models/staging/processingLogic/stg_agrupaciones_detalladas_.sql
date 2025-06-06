--{% set nivel_maximo = get_max_nivel() %}

with datos_nivel_1 as (
    select
        agrupacion,
        concepto,
        numero_cuenta,
        mes,
        anio,
        periodo,
        sum(importe) as importe,
        1 as tipo_nivel
    from {{ ref('stg_datos_detallados') }}
    group by 1,2,3,4,5,6
),

{% set nivel_maximo = 5 %}

{% for nivel_actual in range(2, nivel_maximo + 1) %}

    {% if nivel_actual == 2 %}
        {% set input_nivel = 'datos_nivel_1' %}
    {% else %}
        {% set input_nivel = 'datos_nivel_' ~ (nivel_actual - 1) %}
    {% endif %}

datos_nivel_{{ nivel_actual }} as (
    select
        a.concepto_calculado as agrupacion,
        a.concepto_calculado as concepto,
        a.concepto_calculado as numero_cuenta,
        s.mes,
        s.anio,
        s.periodo,        
        case
            when lower(a.tipo_calculo) = 'suma' then coalesce(s.total_suma, 0) + coalesce(r.total_resta, 0)
            when lower(a.tipo_calculo) = 'resta' then coalesce(s.total_suma, 0) - coalesce(r.total_resta, 0)
            when lower(a.tipo_calculo) = 'division' then 
                case when coalesce(r.total_resta, 0) = 0 then null
                else (coalesce(s.total_suma, 0) / nullif(coalesce(r.total_resta, 0), 0)) * 100 end
            else null
        end as importe,
        {{ nivel_actual }} as tipo_nivel,
    from {{ ref('inter_agrupaciones_niveladas') }} a
    left join (
        select
            concepto_calculado,
            periodo,
            mes,
            anio,
            sum(importe) as total_suma
        from (
            select
                concepto_calculado,
                lower(value::string) as concepto_dependiente
            from {{ ref('inter_agrupaciones_niveladas') }},
                 lateral flatten(input => split(concepto_suma, ';'))
            where nivel = {{ nivel_actual }}
        ) deps
        join {{ input_nivel }} i on deps.concepto_dependiente = lower(i.agrupacion)
        group by 1,2,3,4
    ) s on a.concepto_calculado = s.concepto_calculado --and s.periodo = p.periodo
    left join (
        select
            concepto_calculado,
            periodo,
            mes,
            anio,            
            sum(importe) as total_resta
        from (
            select
                concepto_calculado,
                lower(value::string) as concepto_dependiente
            from {{ ref('inter_agrupaciones_niveladas') }},
                 lateral flatten(input => split(concepto_resta, ';'))
            where nivel = {{ nivel_actual }}
        ) deps
        join {{ input_nivel }} i on deps.concepto_dependiente = lower(i.agrupacion)
        group by 1,2,3,4
    ) r on a.concepto_calculado = r.concepto_calculado and r.periodo = s.periodo
    where a.nivel = {{ nivel_actual }}
)

{% if not loop.last %}
,
{% endif %}

{% endfor %}

, union_final as (
    select
        agrupacion,
        concepto,
        numero_cuenta,
        mes,
        anio,
        periodo,
        importe,
        1 as tipo_nivel
    from {{ ref('stg_datos_detallados') }}
    {% for nivel in range(2, nivel_maximo + 1) %}
    union all
    select * from datos_nivel_{{ nivel }}
    {% endfor %}
),

--Orden personalizado para presentación
orden_agrupaciones as (
   select column1 as agrupacion, column2 as orden_nivel2 from values
        ('Revenues from rent', 1),
        ('Non oprational rent expenses', 2),
        ('Profit from rent', 3),
        ('Revenues from operations', 4),
        ('Direct Costs from operations', 5),        
        ('Gross margin value', 6),
        ('Gross margin rate (%)', 7),
        ('Indirect cost', 8),
        ('% indirect / sales', 9),
        ('E B I T D A', 10),
        ('% EBITDA / sales', 11),    
        ('Amortization', 12),
        ('Financial Cost', 13),
        ('Operational business result', 14),
        ('BUSINESS PERFORMANCE', 15),
        ('% Business performance', 16)
)

select
    u.agrupacion,
    u.concepto,
    u.numero_cuenta,
    u.mes,
    u.anio,
    u.importe,
    u.periodo,
    u.tipo_nivel,
    o.orden_nivel2
from union_final u
left join orden_agrupaciones o on lower(u.agrupacion) = lower(o.agrupacion)
order by periodo, tipo_nivel, orden_nivel2 nulls last, agrupacion
