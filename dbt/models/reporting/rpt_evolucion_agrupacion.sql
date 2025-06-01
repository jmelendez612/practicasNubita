with base as (
    select * from {{ ref('stg_datos_detallados') }}
)

select
    agrupacion,
    periodo,
    sum(importe) as total_agrupacion
from base
group by agrupacion, periodo
order by agrupacion
