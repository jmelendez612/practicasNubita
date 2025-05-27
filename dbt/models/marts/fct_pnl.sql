with base_conceptos as (
    select * from {{ ref('stg_datos_conceptos') }}
),

agrupaciones_expandido as (
    select
        agrupacion,
        trim(value::string) as concepto_unit
    from {{ ref('stg_agrupaciones') }},
    lateral flatten(input => split(conceptos, ';'))
)

select
    a.agrupacion,
    b.concepto,
    b.numero_cuenta,
    b.mes,
    b.anio,
    b.importe,
    TO_CHAR(DATE_FROM_PARTS(b.anio, b.mes, 1), 'Mon YYYY') AS periodo,
    b.anio * 100 + b.mes AS periodo_orden
from base_conceptos b
join agrupaciones_expandido a
    on b.concepto = a.concepto_unit
