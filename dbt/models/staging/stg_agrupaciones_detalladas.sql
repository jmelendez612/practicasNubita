with 
base as (
    select
        lower(coalesce(agrupacion, concepto)) as concepto_base,
        periodo,
        sum(importe) as total
    from {{ ref('stg_datos_detallados') }}
    group by 1, 2
),

agrupaciones as (
    select * from {{ ref('stg_agrupaciones_calculadas') }}
),

periodos as (
    select distinct periodo from base
),

-- Explota conceptos_suma
conceptos_suma as (
    select
        agrupacion_calculada,
        tipo_calculo,
        lower(value::string) as concepto_base
    from agrupaciones,
         lateral flatten(input => split(conceptos_suma, ';'))
),

-- Explota conceptos_resta
conceptos_resta as (
    select
        agrupacion_calculada,
        tipo_calculo,
        lower(value::string) as concepto_base
    from agrupaciones,
         lateral flatten(input => split(conceptos_resta, ';'))
),

-- Suma de conceptos_suma por agrupación y periodo
sumas as (
    select
        cs.agrupacion_calculada,
        b.periodo,
        sum(b.total) as total_suma
    from conceptos_suma cs
    join base b on cs.concepto_base = b.concepto_base
    group by 1, 2
),

-- Suma de conceptos_resta por agrupación y periodo
restas as (
    select
        cr.agrupacion_calculada,
        b.periodo,
        sum(b.total) as total_resta
    from conceptos_resta cr
    join base b on cr.concepto_base = b.concepto_base
    group by 1, 2
),

-- Cálculo de agrupaciones calculadas
calculos as (
    select
        a.agrupacion_calculada,
        p.periodo,
        a.tipo_calculo,
        coalesce(s.total_suma, 0) as total_suma,
        coalesce(r.total_resta, 0) as total_resta,

        case
            when a.tipo_calculo = 'resta' then coalesce(s.total_suma, 0) - coalesce(r.total_resta, 0)
            when a.tipo_calculo = 'division' then 
                case 
                    when coalesce(r.total_resta, 0) = 0 then null
                    else (coalesce(s.total_suma, 0) / nullif(coalesce(r.total_resta, 0), 0)) *100
                end
            when a.tipo_calculo = 'suma' then coalesce(s.total_suma, 0)
            else null
        end as importe
    from agrupaciones a
    cross join periodos p
    left join sumas s on a.agrupacion_calculada = s.agrupacion_calculada and p.periodo = s.periodo
    left join restas r on a.agrupacion_calculada = r.agrupacion_calculada and p.periodo = r.periodo
),

-- Datos reales
reales as (
    select
        coalesce(agrupacion, concepto) as agrupacion,
        concepto,
        numero_cuenta,
        mes,
        anio,
        importe,
        periodo,
        --sum(importe) as importe,
        'real' as tipo_nivel
    from {{ ref('stg_datos_detallados') }}
    --group by 1, 2
),

-- Unificación real + calculado
union_completo as (
    select 
        agrupacion,
        concepto,
        numero_cuenta,
        mes,
        anio,
        importe,
        periodo,
        tipo_nivel
    from reales

    union all

    select
        agrupacion_calculada as agrupacion,
        null as concepto,
        null as numero_cuenta,
        null as mes,
        null as anio,
        importe,        
        periodo,
        'calculado' as tipo_nivel
    from calculos
),

-- Orden personalizado (puedes agregar más filas aquí)
orden_agrupaciones as (
   select column1 as agrupacion, column2 as orden_nivel2 from values
        ('Revenues from rent', 1),
        ('Non oprational rent expenses', 2),
        ('Profit from rent', 3),
        ('Revenues from operations', 4),
        ('Direct Costs from operations', 5),        
        ('Gross margin value', 6),
        ('Gross margin rate', 7),
        ('Indirect cost', 8),
        ('% indirect / sales', 9),
        ('E B I T D A', 10),
        ('% EBITDA / sales', 11),    
        ('Amortization', 12),
        ('Financial Cost', 13),
        ('Operational business result', 14),
        ('BUSINESS PERFORMANCE', 15),
        ('% Business performance', 16)
),

-- Resultado final
resultado_final as (
    select 
        u.agrupacion,
        u.concepto,
        u.numero_cuenta,
        u.mes,
        u.anio,
        u.periodo,
        u.importe,
        u.tipo_nivel,
        o.orden_nivel2  as orden_nivel2,
        initcap(u.agrupacion) as agrupacion_visible
    from union_completo u
    left join orden_agrupaciones o on lower(u.agrupacion) = lower(o.agrupacion)
)

-- Salida final compatible con tu modelo
select
    agrupacion as agrupacion,
    concepto,
    numero_cuenta,
    mes,
    anio,
    importe,
    periodo,
    tipo_nivel,
    orden_nivel2    
from resultado_final
