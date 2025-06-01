with conceptos_base as (
    select * from {{ ref('stg_conceptos_base') }}
),

--Expandir cada concepto con cada valor de inclusión
inclusion_expanded as (
    select
        concepto,
        trim(value) as inclusion_patron,
        exclusion
    from  conceptos_base,
         lateral flatten(input => split(inclusion, ';'))
),

--Expandir cada valor de exclusión, si es q hay
exclusion_expanded as (
    select
        concepto,
        trim(value) as exclusion_cuenta
    from conceptos_base,
         lateral flatten(input => split(coalesce(exclusion, ''),';'))
)

--Consolidar conceptos incluidos y excluidos
select
    i.concepto,
    i.inclusion_patron,
    e.exclusion_cuenta
from inclusion_expanded i
left join exclusion_expanded e
    on i.concepto = e.concepto
