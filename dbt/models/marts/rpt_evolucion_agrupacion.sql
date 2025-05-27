with base as (
    select * from {{ ref('fct_pnl') }}
)

select
    agrupacion,
    periodo,
    periodo_orden,
    sum(importe) as total_agrupacion
from base
group by agrupacion, periodo, periodo_orden
order by agrupacion, periodo_orden
