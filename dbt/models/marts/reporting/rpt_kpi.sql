with base as (
    select * from {{ ref('rpt_datos_completados') }}
),

agregado as (
    select
        periodo,
        mes,
        anio,
        sum(case when agrupacion ='E B I T D A' then importe else 0 end) as "E B I T D A",
        sum(case when agrupacion ='Revenues from operations' then importe else 0 end) as "Revenues from operations",
        sum(case when agrupacion ='BUSINESS PERFORMANCE' then importe else 0 end) as "BUSINESS PERFORMANCE",
        
        sum(case when agrupacion = 'Gross margin rate (%)' then importe else null end)
        / nullif(count(case when agrupacion = 'Gross margin rate (%)' then 1 else null end), 0)
        as "Gross margin rate (%)",

        sum(case when agrupacion = '% EBITDA / sales' then importe else null end)
        / nullif(count(case when agrupacion = '% EBITDA / sales' then 1 else null end), 0)
        as "% EBITDA / sales",

        sum(case when agrupacion = '% Business performance' then importe else null end)
        / nullif(count(case when agrupacion = '% Business performance' then 1 else null end), 0)
        as "% Business performance"
    from base
    group by periodo, mes, anio
),

resumen as (
    select
        a.periodo,
        a.mes,
        a.anio,
        a."E B I T D A",
        a."Revenues from operations",
        a."BUSINESS PERFORMANCE",
        a."Gross margin rate (%)",
        a."% EBITDA / sales",
        a."% Business performance",
        b."E B I T D A" as ebitda_prev_year,
        b."BUSINESS PERFORMANCE" as performance_prev_year
    from agregado a
    left join agregado b
        on a.mes = b.mes and a.anio = b.anio + 1
)

select * from resumen
