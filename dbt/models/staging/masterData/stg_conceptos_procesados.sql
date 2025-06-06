with conceptos_base as (
    select * from {{ ref('stg_conceptos_base') }}
),

--Expandir cada concepto con cada valor de inclusión
inclusion_expanded as (
    select
        concepto,
        'inclusion' as tipo_regla,
        trim(value) as inclusion_patron        
    from  conceptos_base,
         lateral flatten(input => split(inclusion, ';'))
),

--Expandir cada concepto con cada valor de inclusión
exclusion_expanded  as (
    select
        concepto,
        'exclusion' as tipo_regla,
        trim(value) as exclusion_patron
    from conceptos_base,
         lateral flatten(input => split(coalesce(exclusion, ''),';'))
    where exclusion is not null and exclusion != ''
),

--Consolidar conceptos incluidos y excluidos
conceptos_procesados as (
    select 
        concepto,
        tipo_regla,
        inclusion_patron as cuentas
    from inclusion_expanded
    union all
    select 
        concepto,
        tipo_regla,
        exclusion_patron as cuentas
    from exclusion_expanded
)

select *
from conceptos_procesados