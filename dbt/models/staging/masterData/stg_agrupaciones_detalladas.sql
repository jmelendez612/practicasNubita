with fuente as (
    select * from {{ ref('stg_agrupaciones_calculadas') }}
),

suma_partes as (
    select 
        agrupacion_calculada,
        tipo_calculo,
        trim(value) as concepto_parte,
        'suma' as tipo_parte
    from fuente,
    lateral flatten(input => split(conceptos_suma, ';'))
    where conceptos_suma is not null
),

resta_partes as (
    select 
        agrupacion_calculada,
        tipo_calculo,
        trim(value) as concepto_parte,
        'resta' as tipo_parte
    from fuente,
    lateral flatten(input => split(conceptos_resta, ';'))
    where conceptos_resta is not null
)

select * from suma_partes
union all
select * from resta_partes
