select
    numero_cuenta,
    cast(mes as integer) as mes,
    cast(anio as integer) as anio,
    cast(importe as integer) as importe
from {{ ref('datos_financieros') }}