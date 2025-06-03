with 

-- Base inicial con totales por concepto_base y periodo (desde los datos detallados originales)
base_inicial as (
    select
        lower(coalesce(agrupacion, concepto)) as concepto_base,
        periodo,
        sum(importe) as total
    from {{ ref('stg_datos_detallados') }}
    group by 1, 2
),

-- Agrupaciones calculadas (definición de reglas para cálculo)
agrupaciones as (
    select * from {{ ref('stg_agrupaciones_calculadas') }}
),

-- Periodos distintos (para combinaciones posteriores)
periodos as (
    select distinct periodo from base_inicial
),

-- Explota conceptos_suma de agrupaciones
conceptos_suma as (
    select
        agrupacion_calculada,
        tipo_calculo,
        lower(value::string) as concepto_base
    from agrupaciones,
         lateral flatten(input => split(conceptos_suma, ';'))
),

-- Explota conceptos_resta de agrupaciones
conceptos_resta as (
    select
        agrupacion_calculada,
        tipo_calculo,
        lower(value::string) as concepto_base
    from agrupaciones,
         lateral flatten(input => split(conceptos_resta, ';'))
),

-- Calcula sumas por agrupación y periodo
sumas as (
    select
        cs.agrupacion_calculada,
        b.periodo,
        sum(b.total) as total_suma
    from conceptos_suma cs
    join base_inicial b on cs.concepto_base = b.concepto_base
    group by 1, 2
),

-- Calcula restas por agrupación y periodo
restas as (
    select
        cr.agrupacion_calculada,
        b.periodo,
        sum(b.total) as total_resta
    from conceptos_resta cr
    join base_inicial b on cr.concepto_base = b.concepto_base
    group by 1, 2
),

-- Cálculo de agrupaciones calculadas (resultado intermedio)
calculos_inicial as (
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
            when a.tipo_calculo = 'suma' then coalesce(s.total_suma, 0) + coalesce(r.total_resta, 0)
            else null
        end as importe
    from agrupaciones a
    cross join periodos p
    left join sumas s on a.agrupacion_calculada = s.agrupacion_calculada and p.periodo = s.periodo
    left join restas r on a.agrupacion_calculada = r.agrupacion_calculada and p.periodo = r.periodo
),

-- Resultado agrupaciones detalladas (unión de datos reales con calculados)
union_detallada as (
    -- Datos reales
    select
        coalesce(agrupacion, concepto) as agrupacion,
        concepto,
        numero_cuenta,
        mes,
        anio,
        importe,
        periodo,
        'real' as tipo_nivel
    from {{ ref('stg_datos_detallados') }}

    union all

    -- Calculados iniciales
    select
        agrupacion_calculada as agrupacion,
        null as concepto,
        null as numero_cuenta,
        null as mes,
        null as anio,
        importe,
        periodo,
        'calculado' as tipo_nivel
    from calculos_inicial
),

-- Ahora partimos de union_detallada para recalcular las agrupaciones (similar a stg_agrupaciones_detalladas2)
base_secundaria as (
    select
        lower(coalesce(agrupacion, concepto)) as concepto_base,
        periodo,
        sum(importe) as total
    from union_detallada
    group by 1, 2
),

-- Repetimos las tablas auxiliares para las agrupaciones, periodo, conceptos_suma y conceptos_resta
-- Ya definidos arriba, podemos reutilizar directamente agrupaciones y periodos

-- Recalculamos sumas para la segunda capa
sumas_secundarias as (
    select
        cs.agrupacion_calculada,
        b.periodo,
        sum(b.total) as total_suma
    from conceptos_suma cs
    join base_secundaria b on cs.concepto_base = b.concepto_base
    group by 1, 2
),

-- Recalculamos restas para la segunda capa
restas_secundarias as (
    select
        cr.agrupacion_calculada,
        b.periodo,
        sum(b.total) as total_resta
    from conceptos_resta cr
    join base_secundaria b on cr.concepto_base = b.concepto_base
    group by 1, 2
),

-- Cálculo final de agrupaciones (segunda capa)
calculos_finales as (
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
            when a.tipo_calculo = 'suma' then coalesce(s.total_suma, 0) + coalesce(r.total_resta, 0)
            else null
        end as importe
    from agrupaciones a
    cross join periodos p
    left join sumas_secundarias s on a.agrupacion_calculada = s.agrupacion_calculada and p.periodo = s.periodo
    left join restas_secundarias r on a.agrupacion_calculada = r.agrupacion_calculada and p.periodo = r.periodo
),

-- Unión final con datos reales y cálculos finales
union_final as (
    select 
        agrupacion,
        concepto,
        numero_cuenta,
        mes,
        anio,
        importe,
        periodo,
        tipo_nivel
    from union_detallada
    where tipo_nivel <> 'calculado'

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
    from calculos_finales
),

-- Orden personalizado para presentación
orden_agrupaciones as (
   select column1 as agrupacion, column2 as orden_nivel2 from values
        ('Revenues from rent', 1),
        ('Non oprational rent expenses', 2),
        ('Profit from rent', 3),
        ('Revenues from operations', 4),
        ('Direct Costs from operations', 5),        
        ('Gross margin value', 6),
        ('Gross margin rate (%)', 7),
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

-- Resultado final ordenado y listo para consumir
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
    from union_final u
    left join orden_agrupaciones o on lower(u.agrupacion) = lower(o.agrupacion)
)

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
from resultado_final
order by periodo, orden_nivel2 nulls last, agrupacion
