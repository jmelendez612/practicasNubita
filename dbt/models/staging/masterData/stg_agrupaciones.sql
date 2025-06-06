select
    trim(agrupacion) as agrupacion,
    trim(conceptos) as conceptos
from {{ ref('agrupaciones') }}