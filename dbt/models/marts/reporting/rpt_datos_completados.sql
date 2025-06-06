select
    agrupacion,
    concepto,
    numero_cuenta,
    mes,
    anio,
    importe,
    periodo,
    case 
        when tipo_nivel = 0 then 'real'
        else 'calculado'
    end as tipo_nivel,
    orden_nivel2
from {{ ref('inter_calculo_niveles') }}
