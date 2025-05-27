select
    agrupacion,
    trim(conceptos) as conceptos
from {{ ref('agrupaciones') }}