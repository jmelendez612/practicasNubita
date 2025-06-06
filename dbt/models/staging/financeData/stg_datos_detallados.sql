with datos_financieros as (
    select * from {{ ref('stg_datos_financieros') }}
),

conceptos_procesados as (
    select * from {{ ref('stg_conceptos_procesados') }}
),

inclusiones as (
    select * 
    from conceptos_procesados
    where tipo_regla = 'inclusion'
),

exclusiones as (
    select * 
    from conceptos_procesados
    where tipo_regla = 'exclusion'
),

cuentas_con_concepto as (
    select
        i.concepto,
        d.numero_cuenta,
        d.mes,
        d.anio,
        d.importe
    from datos_financieros d
    join inclusiones i
        on (
            (right(i.cuentas, 1) = '*' and d.numero_cuenta like left(i.cuentas, len(i.cuentas) - 1) || '%')
            or
            (right(i.cuentas, 1) != '*' and d.numero_cuenta = i.cuentas)
        )
    left join exclusiones e
        on i.concepto = e.concepto
        and d.numero_cuenta = e.cuentas
    where e.cuentas is null
),

agrupaciones_expandido as (
    select
        trim(a.agrupacion) as agrupacion,
        trim(value::string) as concepto
    from {{ ref('stg_agrupaciones') }} a,
         lateral flatten(input => split(a.conceptos, ';'))
),

resultado_final as (
    select
        ae.agrupacion,
        c.concepto,
        c.numero_cuenta,
        c.mes,
        c.anio,
        c.importe,
        TO_CHAR(DATE_FROM_PARTS(c.anio, c.mes, 1), 'Mon YYYY') AS periodo,
        1 as tipo_nivel, 
        0 as orden_nivel2
    from cuentas_con_concepto c
    left join agrupaciones_expandido ae
        on lower(c.concepto) = lower(ae.concepto)
)

select *
from resultado_final
order by agrupacion, concepto, numero_cuenta
