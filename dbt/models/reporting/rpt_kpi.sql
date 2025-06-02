with base as (
    select * from {{ ref('rpt_datos_completados') }}
),

resumen as (
    select
        periodo,
        sum(case when agrupacion ='E B I T D A' then importe else 0 end) as EBITDA,
        sum(case when agrupacion ='Operational business result' then importe else 0 end) as Operational_Business_Result,
        sum(case when agrupacion ='Non oprational rent expenses' then importe else 0 end) as Non_oprational_rent_expenses,
        sum(case when agrupacion ='BUSINESS PERFORMANCE' then importe else 0 end) as BUSINESS_PERFORMANCE,
        round(
            nullif(sum(case when agrupacion = 'INGRESOS' then importe else 0 end), 0) /
            nullif(sum(importe), 0) * 100, 2
        ) as margen_pct
    from base
    group by periodo
)

select * from resumen
