{% macro calcular_todos_los_niveles(nivel_max=5) %}

with

--Nivel 1, datos sin dependencias
nivel_1 as (
    select
        AGRUPACION,
        MES,
        ANIO,
        PERIODO,
        sum(IMPORTE) as IMPORTE,
        1 as NIVEL
    from {{ ref('stg_datos_detallados') }}
    group by 1,2,3,4
),

acumulado_1 as (
    select * from nivel_1
)

{% for nivel in range(2, nivel_max + 1) %}

, nivel_{{ nivel }} as (
    select
        SAD.AGRUPACION_CALCULADA as AGRUPACION,
        T.MES,
        T.ANIO,
        T.PERIODO,
        case 
            when SAD.TIPO_CALCULO = 'resta' then 
                sum(case when SAD.TIPO_PARTE = 'suma' then T.IMPORTE else 0 end)
                - sum(case when SAD.TIPO_PARTE = 'resta' then T.IMPORTE else 0 end)
            when SAD.TIPO_CALCULO = 'suma' then 
                sum(case when SAD.TIPO_PARTE = 'suma' then T.IMPORTE else 0 end)
                + sum(case when SAD.TIPO_PARTE = 'resta' then T.IMPORTE else 0 end)
            when SAD.TIPO_CALCULO = 'division' then 
                case 
                    when sum(case when SAD.TIPO_PARTE = 'resta' then T.IMPORTE else 0 end) = 0 then null
                    else round(
                        100.0 * sum(case when SAD.TIPO_PARTE = 'suma' then T.IMPORTE else 0 end)
                        / nullif(sum(case when SAD.TIPO_PARTE = 'resta' then T.IMPORTE else 0 end), 0), 2)
                end
            else null
        end as IMPORTE,
        {{ nivel }} as NIVEL
    from {{ ref('stg_agrupaciones_detalladas') }} SAD
    join acumulado_{{ nivel - 1 }} T
        on T.AGRUPACION = SAD.CONCEPTO_PARTE
    join {{ ref('inter_agrupaciones_niveladas') }} IAN
        on SAD.AGRUPACION_CALCULADA = IAN.CONCEPTO_CALCULADO
    where IAN.NIVEL = {{ nivel }}
    group by 1,2,3,4, SAD.TIPO_CALCULO
),

--Acumlar resultados previos
acumulado_{{ nivel }} as (
    select * from acumulado_{{ nivel - 1 }}
    union all
    select * from nivel_{{ nivel }}
)

{% endfor %},

--Orden personalizado para el dash --JM requiere revisión con usuario para una tabla maestra
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
),

union_final as (
    select
        agrupacion,
        agrupacion as concepto,
        agrupacion as numero_cuenta,
        mes,
        anio,
        periodo,
        importe,
        nivel as tipo_nivel,
    from acumulado_{{ nivel_max }}
    where nivel <> 1
    union all
    select
        agrupacion,
        concepto,
        numero_cuenta,
        mes,
        anio,
        periodo,
        importe,
        tipo_nivel
    from {{ ref('stg_datos_detallados') }}
)    

select 
    u.agrupacion,
    u.concepto,
    u.numero_cuenta,
    u.mes,
    u.anio,
    u.periodo,
    u.importe,
    u.tipo_nivel,
    o.orden_nivel2  as orden_nivel2,
from union_final u
left join orden_agrupaciones o on lower(u.agrupacion) = lower(o.agrupacion)
order by o.orden_nivel2 nulls last, u.agrupacion, u.mes, u.anio, u.periodo

{% endmacro %}