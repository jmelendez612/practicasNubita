select
    agrupacion,
    concepto,
    numero_cuenta,
    mes,
    anio,
    importe,
    periodo,
    tipo_nivel,
    orden_nivel2
from {{ ref('stg_agrupaciones_detalladas') }}
