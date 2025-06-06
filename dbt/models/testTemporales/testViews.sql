select agrupacion_calculada, tipo_calculo, concepto_parte, tipo_parte
from {{ ref('stg_agrupaciones_detalladas') }}