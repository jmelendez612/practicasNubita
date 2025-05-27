select
    agrupacion,
    concepto,
    numero_cuenta,
    periodo,
    importe
from {{ ref('fct_pnl') }}
