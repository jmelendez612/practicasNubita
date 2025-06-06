select
    trim(concepto) as concepto,
    trim(inclusion) as inclusion,
    trim(exclusion) as exclusion
from {{ ref('conceptos_base') }}