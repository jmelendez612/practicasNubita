with datos_financieros as (
    select * from {{ ref('stg_datos_financieros') }}
),

conceptos_procesados as (
    select * from {{ ref('stg_conceptos_procesados') }}
),

cuentas_con_concepto as (
    select
        c.concepto,
        d.numero_cuenta,
        d.mes,
        d.anio,
        d.importe,
        c2.exclusion_cuenta
    from datos_financieros d
    join conceptos_procesados c
        on (
            (right(c.inclusion_patron, 1) = '*' and d.numero_cuenta like left(c.inclusion_patron, len(c.inclusion_patron) - 1) || '%')
            or
            (right(c.inclusion_patron, 1) != '*' and d.numero_cuenta = c.inclusion_patron)
        )
    left join conceptos_procesados c2
        on c.concepto = c2.concepto
       and d.numero_cuenta = c2.exclusion_cuenta
    where c2.exclusion_cuenta is null
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
        'real' as tipo_nivel, 
        null as orden_nivel2
    from cuentas_con_concepto c
    left join agrupaciones_expandido ae
        on lower(c.concepto) = lower(ae.concepto)
)

select *
from resultado_final
order by agrupacion, concepto, numero_cuenta
