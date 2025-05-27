with base as (
    select * from {{ ref('fct_pnl') }}
)

select
    current_date() as fecha_consulta,
    sum(case when agrupacion ilike '%ingreso%' then importe else 0 end) as total_ingresos,
    sum(case when agrupacion ilike '%gasto%' then importe else 0 end) as total_gastos,
    sum(case when agrupacion ilike '%ingreso%' then importe else 0 end)
    - sum(case when agrupacion ilike '%gasto%' then importe else 0 end) as resultado_neto
from base
