-- models/transform/pnl_final.sql

with base_conceptos as (
    select * from {{ ref('concepto_base_transformado') }}
),

agrupaciones_expandido as (
    select
        agrupacion,
        trim(value::string) as concepto_unit
    from {{ ref('agrupaciones') }},
    lateral flatten(input => split(conceptos, ';'))
)

select
    a.agrupacion,
    b.concepto,
    b.mes,
    b.anio,
    b.importe,
    TO_CHAR(DATE_FROM_PARTS(b.anio, b.mes, 1), 'Mon YYYY') AS periodo,
    b.anio * 100 + b.mes AS periodo_orden    
from base_conceptos b
join agrupaciones_expandido a
    on b.concepto = a.concepto_unit
