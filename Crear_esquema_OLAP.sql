-- Primero creamos el esquema OLAP
CREATE SCHEMA IF NOT EXISTS olap;

-- tabla Dimensión : tiempo
CREATE TABLE olap.dim_tiempo (
    tiempo_sk     SERIAL PRIMARY KEY,
    fecha         DATE,
    anio          INT,
    mes           INT,
    nombre_mes    VARCHAR(20),
    trimestre     INT,
    semana        INT,
    dia_semana    INT,
    nombre_dia    VARCHAR(20),
    temporada     VARCHAR(10)
);

-- La poblamos con todas las fechas de partidos y entrenamientos
INSERT INTO olap.dim_tiempo (
    fecha, anio, mes, nombre_mes,
    trimestre, semana, dia_semana,
    nombre_dia, temporada
)
SELECT DISTINCT
    fecha::DATE,
    EXTRACT(YEAR FROM fecha)::INT,
    EXTRACT(MONTH FROM fecha)::INT,
    TO_CHAR(fecha, 'TMMonth'),
    EXTRACT(QUARTER FROM fecha)::INT,
    EXTRACT(WEEK FROM fecha)::INT,
    EXTRACT(ISODOW FROM fecha)::INT,
    TO_CHAR(fecha, 'TMDay'),
    '2024-25'
FROM (
    SELECT fecha_partido AS fecha FROM oltp_partidos.partidos
    UNION
    SELECT fecha_sesion  AS fecha FROM oltp_entrenamientos.entrenamientos
) fechas;

-- tabla Dimensión : jugador
CREATE TABLE olap.dim_jugador (
    jugador_sk        SERIAL PRIMARY KEY,
    jugador_id        INT,
    nombre            VARCHAR(100),
    apellidos         VARCHAR(100),
    nombre_completo   VARCHAR(200),
    edad              INT,
    nacionalidad      VARCHAR(50),
    posicion          VARCHAR(50),
    dorsal            INT,
    peso_kg           NUMERIC(5,1),
    altura_cm         NUMERIC(5,1),
    pie_dominante     VARCHAR(20),
    nombre_club       VARCHAR(100),
    liga              VARCHAR(50),
    pais_club         VARCHAR(50)
);

INSERT INTO olap.dim_jugador (
    jugador_id, nombre, apellidos, nombre_completo,
    edad, nacionalidad, posicion, dorsal,
    peso_kg, altura_cm, pie_dominante,
    nombre_club, liga, pais_club
)
SELECT
    j.id_jugador,
    j.nombre,
    j.apellidos,
    j.nombre || ' ' || j.apellidos,
    DATE_PART('year', AGE(j.fecha_nacimiento))::INT,
    j.nacionalidad,
    j.posicion,
    j.dorsal,
    j.peso_kg,
    j.altura_cm,
    j.pie_dominante,
    c.nombre_club,
    c.liga,
    c.pais
FROM oltp_jugadores.jugadores j
JOIN oltp_clubes.clubes c ON j.id_club = c.id_club;

-- tabla Dimensión : club
CREATE TABLE olap.dim_club (
    club_sk        SERIAL PRIMARY KEY,
    club_id        INT,
    nombre_club    VARCHAR(100),
    pais           VARCHAR(50),
    ciudad         VARCHAR(50),
    liga           VARCHAR(50),
    num_jugadores  INT,
    fecha_contrato DATE
);

INSERT INTO olap.dim_club (
    club_id, nombre_club, pais,
    ciudad, liga, num_jugadores,
    fecha_contrato
)
SELECT
    id_club,
    nombre_club,
    pais,
    ciudad,
    liga,
    num_jugadores,
    fecha_contrato
FROM oltp_clubes.clubes
WHERE activo = TRUE;


-- tabla Dimensión : partido
CREATE TABLE olap.dim_partido (
    partido_sk       SERIAL PRIMARY KEY,
    partido_id       INT,
    competicion      VARCHAR(50),
    jornada          VARCHAR(50),
    local_visitante  VARCHAR(20),
    duracion_min     INT,
    temporada        VARCHAR(10),
    fecha            DATE,
    nombre_club      VARCHAR(100)
);

INSERT INTO olap.dim_partido (
    partido_id, competicion, jornada,
    local_visitante, duracion_min,
    temporada, fecha, nombre_club
)
SELECT
    p.id_partido,
    p.competicion,
    p.jornada,
    p.local_visitante,
    p.duracion_min,
    p.temporada,
    p.fecha_partido::DATE,
    c.nombre_club
FROM oltp_partidos.partidos p
JOIN oltp_clubes.clubes c ON p.id_club = c.id_club;


-- tabla Dimensión : entrenamiento
CREATE TABLE olap.dim_entrenamiento (
    entrenamiento_sk  SERIAL PRIMARY KEY,
    entrenamiento_id  INT,
    tipo              VARCHAR(50),
    duracion_min      INT,
    intensidad        INT,
    temporada         VARCHAR(10),
    fecha             DATE,
    nombre_club       VARCHAR(100)
);

INSERT INTO olap.dim_entrenamiento (
    entrenamiento_id, tipo, duracion_min,
    intensidad, temporada, fecha, nombre_club
)
SELECT
    e.id_entrenamiento,
    e.tipo_entrenamiento,
    e.duracion_min,
    e.intensidad,
    e.temporada,
    e.fecha_sesion::DATE,
    c.nombre_club
FROM oltp_entrenamientos.entrenamientos e
JOIN oltp_clubes.clubes c ON e.id_club = c.id_club;


-- tabla de hechos: salud
CREATE TABLE olap.fact_salud (
    salud_sk            SERIAL PRIMARY KEY,
    tiempo_sk           INT REFERENCES olap.dim_tiempo(tiempo_sk),
    jugador_sk          INT REFERENCES olap.dim_jugador(jugador_sk),
    club_sk             INT REFERENCES olap.dim_club(club_sk),
    partido_sk          INT REFERENCES olap.dim_partido(partido_sk),
    entrenamiento_sk    INT REFERENCES olap.dim_entrenamiento(entrenamiento_sk),
    minuto              INT,
    frec_cardiaca       INT,
    temperatura         NUMERIC(4,1),
    fatiga              NUMERIC(4,2),
    equilibrio          NUMERIC(4,2),
    riesgo_lesion       NUMERIC(5,1)
);

INSERT INTO olap.fact_salud (
    tiempo_sk, jugador_sk, club_sk,
    partido_sk, entrenamiento_sk,
    minuto, frec_cardiaca, temperatura,
    fatiga, equilibrio, riesgo_lesion
)
SELECT
    dt.tiempo_sk,
    dj.jugador_sk,
    dc.club_sk,
    dp.partido_sk,
    de.entrenamiento_sk,
    ss.minuto,
    ss.frec_cardiaca,
    ss.temperatura,
    ss.fatiga,
    ss.equilibrio,
    ss.riesgo_lesion
FROM oltp_sensor.sensor_salud ss
JOIN oltp_jugadores.jugadores j
    ON ss.id_jugador = j.id_jugador
JOIN olap.dim_jugador dj
    ON dj.jugador_id = ss.id_jugador
JOIN olap.dim_club dc
    ON dc.club_id = j.id_club
LEFT JOIN oltp_partidos.partidos p
    ON ss.id_partido = p.id_partido
LEFT JOIN olap.dim_tiempo dt
    ON dt.fecha = COALESCE(p.fecha_partido::DATE,
       (SELECT fecha_sesion::DATE FROM oltp_entrenamientos.entrenamientos
        WHERE id_entrenamiento = ss.id_entrenamiento))
LEFT JOIN olap.dim_partido dp
    ON dp.partido_id = ss.id_partido
LEFT JOIN olap.dim_entrenamiento de
    ON de.entrenamiento_id = ss.id_entrenamiento;


-- tabla de hechos: rendimiento
CREATE TABLE olap.fact_rendimiento (
    rendimiento_sk      SERIAL PRIMARY KEY,
    tiempo_sk           INT REFERENCES olap.dim_tiempo(tiempo_sk),
    jugador_sk          INT REFERENCES olap.dim_jugador(jugador_sk),
    club_sk             INT REFERENCES olap.dim_club(club_sk),
    partido_sk          INT REFERENCES olap.dim_partido(partido_sk),
    entrenamiento_sk    INT REFERENCES olap.dim_entrenamiento(entrenamiento_sk),
    minuto              INT,
    km_acumulados       NUMERIC(5,2),
    velocidad           NUMERIC(5,1),
    num_sprints         INT,
    aceleraciones       INT,
    frenadas            INT
);

INSERT INTO olap.fact_rendimiento (
    tiempo_sk, jugador_sk, club_sk,
    partido_sk, entrenamiento_sk,
    minuto, km_acumulados, velocidad,
    num_sprints, aceleraciones, frenadas
)
SELECT
    dt.tiempo_sk,
    dj.jugador_sk,
    dc.club_sk,
    dp.partido_sk,
    de.entrenamiento_sk,
    sr.minuto,
    sr.km_acumulados,
    sr.velocidad,
    sr.num_sprints,
    sr.aceleraciones,
    sr.frenadas
FROM oltp_sensor.sensor_rendimiento sr
JOIN oltp_jugadores.jugadores j
    ON sr.id_jugador = j.id_jugador
JOIN olap.dim_jugador dj
    ON dj.jugador_id = sr.id_jugador
JOIN olap.dim_club dc
    ON dc.club_id = j.id_club
LEFT JOIN oltp_partidos.partidos p
    ON sr.id_partido = p.id_partido
LEFT JOIN olap.dim_tiempo dt
    ON dt.fecha = COALESCE(p.fecha_partido::DATE,
       (SELECT fecha_sesion::DATE
        FROM oltp_entrenamientos.entrenamientos
        WHERE id_entrenamiento = sr.id_entrenamiento))
LEFT JOIN olap.dim_partido dp
    ON dp.partido_id = sr.id_partido
LEFT JOIN olap.dim_entrenamiento de
    ON de.entrenamiento_id = sr.id_entrenamiento;

-- tabla de hechos: lesion_riesgo
CREATE TABLE olap.fact_lesion_riesgo (
    lesion_sk           SERIAL PRIMARY KEY,
    tiempo_sk           INT REFERENCES olap.dim_tiempo(tiempo_sk),
    jugador_sk          INT REFERENCES olap.dim_jugador(jugador_sk),
    club_sk             INT REFERENCES olap.dim_club(club_sk),
    semana              INT,
    anio                INT,
    riesgo_medio        NUMERIC(5,1),
    riesgo_maximo       NUMERIC(5,1),
    fatiga_media        NUMERIC(4,2),
    temperatura_media   NUMERIC(4,1),
    desequilibrio_medio NUMERIC(4,2),
    num_alertas         INT,
    en_baja             BOOLEAN
);

INSERT INTO olap.fact_lesion_riesgo (
    tiempo_sk, jugador_sk, club_sk,
    semana, anio,
    riesgo_medio, riesgo_maximo,
    fatiga_media, temperatura_media,
    desequilibrio_medio, num_alertas,
    en_baja
)
SELECT
    dt.tiempo_sk,
    dj.jugador_sk,
    dc.club_sk,
    EXTRACT(WEEK FROM fecha_dato)::INT,
    EXTRACT(YEAR FROM fecha_dato)::INT,
    AVG(ss.riesgo_lesion),
    MAX(ss.riesgo_lesion),
    AVG(ss.fatiga),
    AVG(ss.temperatura),
    AVG(ss.equilibrio),
    COUNT(CASE WHEN ss.riesgo_lesion > 70 THEN 1 END),
    EXISTS (
        SELECT 1 FROM oltp_medico.bajas b
        WHERE b.id_jugador = ss.id_jugador
        AND b.activa = TRUE
    )
FROM oltp_sensor.sensor_salud ss
JOIN oltp_jugadores.jugadores j
    ON ss.id_jugador = j.id_jugador
JOIN olap.dim_jugador dj
    ON dj.jugador_id = ss.id_jugador
JOIN olap.dim_club dc
    ON dc.club_id = j.id_club
JOIN (
    SELECT
        id_jugador,
        COALESCE(
            (SELECT fecha_partido::DATE
             FROM oltp_partidos.partidos
             WHERE id_partido = ss2.id_partido),
            (SELECT fecha_sesion::DATE
             FROM oltp_entrenamientos.entrenamientos
             WHERE id_entrenamiento = ss2.id_entrenamiento)
        ) AS fecha_dato
    FROM oltp_sensor.sensor_salud ss2
) fechas ON fechas.id_jugador = ss.id_jugador
JOIN olap.dim_tiempo dt
    ON dt.fecha = fechas.fecha_dato
GROUP BY
    dt.tiempo_sk,
    dj.jugador_sk,
    dc.club_sk,
    EXTRACT(WEEK FROM fechas.fecha_dato),
    EXTRACT(YEAR FROM fechas.fecha_dato),
    ss.id_jugador;