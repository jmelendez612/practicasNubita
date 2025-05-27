with base as (
    select * from {{ ref('fct_pnl') }}
),

resumen as (
    select
        periodo,
        sum(case when agrupacion = 'INGRESOS' then importe else 0 end) as ingresos,
        sum(case when agrupacion = 'COSTOS' then importe else 0 end) as costos,
        sum(case when agrupacion not in ('INGRESOS', 'COSTOS') then importe else 0 end) as otros,
        sum(importe) as resultado,
        round(
            nullif(sum(case when agrupacion = 'INGRESOS' then importe else 0 end), 0) /
            nullif(sum(importe), 0) * 100, 2
        ) as margen_pct
    from base
    group by periodo
)

select * from resumen
