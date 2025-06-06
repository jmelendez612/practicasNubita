select
    trim(concepto_calculado) as agrupacion_calculada,
    lower(trim(tipo_calculo)) as tipo_calculo,
    trim(concepto_suma) as conceptos_suma,
    trim(concepto_resta) as conceptos_resta
from {{ ref('calculados') }}