with 
conceptos_suma as (
    select 
        trim(concepto_calculado) as agrupacion_calculada,
        lower(trim(tipo_calculo)) as tipo_calculo,
        trim(value::string) as concepto_dependiente
    from {{ ref('calculados') }},
         lateral flatten(input => split(concepto_suma, ';'))
),

conceptos_resta as (
    select 
        trim(concepto_calculado) as agrupacion_calculada,
        lower(trim(tipo_calculo)) as tipo_calculo,
        trim(value::string) as concepto_dependiente
    from {{ ref('calculados') }},
         lateral flatten(input => split(concepto_resta, ';'))
),

dependencias_expandida as (
    select * from conceptos_suma
    union all
    select * from conceptos_resta
),

nodos_con_dependencias as (
    select
        agrupacion_calculada,
        concepto_dependiente
    from dependencias_expandida
),

recursivo as (
    --Nivel base
    select 
        concepto_dependiente as nodo,
        1 as nivel
    from nodos_con_dependencias
    where concepto_dependiente not in (select distinct agrupacion_calculada from nodos_con_dependencias)

    union all

    --Calcular recursividad para los niveles
    select 
        d.agrupacion_calculada as nodo,
        r.nivel + 1 as nivel
    from nodos_con_dependencias d
    join recursivo r
        on d.concepto_dependiente = r.nodo
),

niveles_finales as (
    select 
        nodo as agrupacion_calculada,
        max(nivel) as nivel
    from recursivo
    group by 1
)

select
    c.*,
    coalesce(n.nivel, 1) as nivel
from {{ ref('calculados') }} c
left join niveles_finales n
    on trim(c.concepto_calculado) = trim(n.agrupacion_calculada)
