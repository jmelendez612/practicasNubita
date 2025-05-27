select
    concepto,
    trim(inclusion) as inclusion,
    trim(exclusion) as exclusion
from {{ ref('conceptos_base') }}