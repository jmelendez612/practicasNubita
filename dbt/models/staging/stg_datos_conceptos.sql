with base as (
    select * from {{ ref('stg_datos_financieros') }}
),

conceptos_expandido as (
    select
        concepto,
        inclusion,
        exclusion
    from {{ ref('stg_conceptos_base') }}
),

exclusiones_expandido as (
    select
        concepto,
        trim(value::string) as exclusion_valor
    from conceptos_expandido,
    lateral flatten(input => split(exclusion, ';'))
),

base_conceptos as (
    select
        b.*,
        c.concepto
    from base b
    join conceptos_expandido c
        on b.numero_cuenta like replace(c.inclusion, '*', '%')
    left join exclusiones_expandido e
        on c.concepto = e.concepto
        and b.numero_cuenta = e.exclusion_valor
    where e.exclusion_valor is null
)

select * from base_conceptos
