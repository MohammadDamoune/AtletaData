-- ======================================================================
-- PRÁCTICA 2 — INTEGRACIÓN DE SISTEMAS ETL
-- Alumno: Mohamed Damoune
-- ======================================================================
-- Este script incluye todo el sistema completo:
-- 1. Creación del OLTP
-- 2. Creación del OLAP
-- 3. Creación del Staging
-- 4. ETL en SQL (equivalente al flujo Pentaho)
-- ======================================================================


-- ======================================================================
-- PARTE 1 — OLTP (de la Práctica 1)
-- ======================================================================

CREATE SCHEMA IF NOT EXISTS oltp_clubes;
CREATE SCHEMA IF NOT EXISTS oltp_jugadores;
CREATE SCHEMA IF NOT EXISTS oltp_partidos;
CREATE SCHEMA IF NOT EXISTS oltp_entrenamientos;
CREATE SCHEMA IF NOT EXISTS oltp_sensor;
CREATE SCHEMA IF NOT EXISTS oltp_medico;

CREATE TABLE IF NOT EXISTS oltp_clubes.clubes (
    id_club        SERIAL PRIMARY KEY,
    nombre_club    VARCHAR(100) NOT NULL,
    pais           VARCHAR(60)  NOT NULL,
    ciudad         VARCHAR(60)  NOT NULL,
    liga           VARCHAR(80)  NOT NULL,
    num_jugadores  INTEGER      NOT NULL,
    fecha_contrato DATE         NOT NULL,
    activo         BOOLEAN      DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS oltp_jugadores.jugadores (
    id_jugador       SERIAL PRIMARY KEY,
    id_club          INTEGER      NOT NULL REFERENCES oltp_clubes.clubes(id_club),
    nombre           VARCHAR(60)  NOT NULL,
    apellidos        VARCHAR(80)  NOT NULL,
    fecha_nacimiento DATE         NOT NULL,
    nacionalidad     VARCHAR(60)  NOT NULL,
    posicion         VARCHAR(40)  NOT NULL,
    dorsal           INTEGER      NOT NULL,
    peso_kg          DECIMAL(5,2),
    altura_cm        DECIMAL(5,2),
    pie_dominante    VARCHAR(10)  CHECK (pie_dominante IN ('derecho','izquierdo','ambos')),
    estado           VARCHAR(20)  DEFAULT 'activo' CHECK (estado IN ('activo','lesionado','baja')),
    foto_ruta        VARCHAR(200)
);

CREATE TABLE IF NOT EXISTS oltp_partidos.partidos (
    id_partido      SERIAL PRIMARY KEY,
    id_club         INTEGER      NOT NULL REFERENCES oltp_clubes.clubes(id_club),
    fecha_partido   TIMESTAMP    NOT NULL,
    competicion     VARCHAR(80)  NOT NULL,
    jornada         VARCHAR(40),
    local_visitante VARCHAR(10)  NOT NULL CHECK (local_visitante IN ('local','visitante')),
    duracion_min    INTEGER      DEFAULT 90,
    temporada       VARCHAR(20)  NOT NULL,
    estado          VARCHAR(20)  DEFAULT 'jugado' CHECK (estado IN ('jugado','suspendido','pendiente'))
);

CREATE TABLE IF NOT EXISTS oltp_partidos.alineaciones (
    id_alineacion   SERIAL PRIMARY KEY,
    id_partido      INTEGER NOT NULL REFERENCES oltp_partidos.partidos(id_partido),
    id_jugador      INTEGER NOT NULL REFERENCES oltp_jugadores.jugadores(id_jugador),
    titular         BOOLEAN NOT NULL DEFAULT TRUE,
    posicion_juego  VARCHAR(40) NOT NULL,
    minuto_entrada  INTEGER DEFAULT 0,
    minuto_salida   INTEGER DEFAULT 90
);

CREATE TABLE IF NOT EXISTS oltp_partidos.cambios (
    id_cambio        SERIAL PRIMARY KEY,
    id_partido       INTEGER NOT NULL REFERENCES oltp_partidos.partidos(id_partido),
    id_jugador_sale  INTEGER NOT NULL REFERENCES oltp_jugadores.jugadores(id_jugador),
    id_jugador_entra INTEGER NOT NULL REFERENCES oltp_jugadores.jugadores(id_jugador),
    minuto           INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS oltp_partidos.tarjetas (
    id_tarjeta  SERIAL PRIMARY KEY,
    id_partido  INTEGER NOT NULL REFERENCES oltp_partidos.partidos(id_partido),
    id_jugador  INTEGER NOT NULL REFERENCES oltp_jugadores.jugadores(id_jugador),
    tipo        VARCHAR(10) NOT NULL CHECK (tipo IN ('amarilla','roja')),
    minuto      INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS oltp_entrenamientos.entrenamientos (
    id_entrenamiento   SERIAL PRIMARY KEY,
    id_club            INTEGER      NOT NULL REFERENCES oltp_clubes.clubes(id_club),
    fecha_sesion       TIMESTAMP    NOT NULL,
    tipo_entrenamiento VARCHAR(40)  NOT NULL CHECK (tipo_entrenamiento IN ('fisico','tactico','recuperacion','tecnico','mixto')),
    duracion_min       INTEGER      NOT NULL,
    intensidad         INTEGER      NOT NULL CHECK (intensidad BETWEEN 1 AND 10),
    temporada          VARCHAR(20)  NOT NULL,
    observaciones      TEXT,
    estado             VARCHAR(20)  DEFAULT 'completado' CHECK (estado IN ('completado','cancelado','pendiente'))
);

CREATE TABLE IF NOT EXISTS oltp_sensor.sensor_salud (
    id_registro      SERIAL PRIMARY KEY,
    id_jugador       INTEGER      NOT NULL REFERENCES oltp_jugadores.jugadores(id_jugador),
    id_partido       INTEGER      REFERENCES oltp_partidos.partidos(id_partido),
    id_entrenamiento INTEGER      REFERENCES oltp_entrenamientos.entrenamientos(id_entrenamiento),
    minuto           INTEGER      NOT NULL CHECK (minuto >= 0),
    frec_cardiaca    INTEGER      NOT NULL,
    temperatura      DECIMAL(4,1) NOT NULL,
    fatiga           DECIMAL(4,2) NOT NULL CHECK (fatiga BETWEEN 0 AND 10),
    equilibrio       DECIMAL(5,2),
    riesgo_lesion    DECIMAL(4,2) CHECK (riesgo_lesion BETWEEN 0 AND 100),
    timestamp_real   TIMESTAMP    DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS oltp_sensor.sensor_rendimiento (
    id_registro      SERIAL PRIMARY KEY,
    id_jugador       INTEGER      NOT NULL REFERENCES oltp_jugadores.jugadores(id_jugador),
    id_partido       INTEGER      REFERENCES oltp_partidos.partidos(id_partido),
    id_entrenamiento INTEGER      REFERENCES oltp_entrenamientos.entrenamientos(id_entrenamiento),
    minuto           INTEGER      NOT NULL CHECK (minuto >= 0),
    km_acumulados    DECIMAL(5,2) NOT NULL,
    velocidad        DECIMAL(5,2) NOT NULL,
    num_sprints      INTEGER      DEFAULT 0,
    aceleraciones    INTEGER      DEFAULT 0,
    frenadas         INTEGER      DEFAULT 0,
    timestamp_real   TIMESTAMP    DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS oltp_medico.historial_medico (
    id_lesion          SERIAL PRIMARY KEY,
    id_jugador         INTEGER      NOT NULL REFERENCES oltp_jugadores.jugadores(id_jugador),
    tipo_lesion        VARCHAR(80)  NOT NULL,
    zona_cuerpo        VARCHAR(60)  NOT NULL,
    gravedad           VARCHAR(20)  NOT NULL CHECK (gravedad IN ('leve','moderada','grave')),
    fecha_inicio       DATE         NOT NULL,
    fecha_alta         DATE,
    dias_descanso      INTEGER      NOT NULL,
    tratamiento        TEXT,
    medico_responsable VARCHAR(100) NOT NULL,
    detectada_sensor   BOOLEAN      DEFAULT FALSE,
    observaciones      TEXT
);

CREATE TABLE IF NOT EXISTS oltp_medico.bajas (
    id_baja      SERIAL PRIMARY KEY,
    id_jugador   INTEGER NOT NULL REFERENCES oltp_jugadores.jugadores(id_jugador),
    fecha_inicio DATE    NOT NULL,
    fecha_fin    DATE,
    motivo       VARCHAR(100) NOT NULL,
    medico       VARCHAR(100) NOT NULL,
    activa       BOOLEAN DEFAULT TRUE
);


-- ======================================================================
-- PARTE 2 — OLAP (de la Práctica 1)
-- ======================================================================

CREATE SCHEMA IF NOT EXISTS olap;

CREATE TABLE IF NOT EXISTS olap.dim_tiempo (
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

CREATE TABLE IF NOT EXISTS olap.dim_jugador (
    jugador_sk      SERIAL PRIMARY KEY,
    jugador_id      INT,
    nombre          VARCHAR(100),
    apellidos       VARCHAR(100),
    nombre_completo VARCHAR(200),
    edad            INT,
    nacionalidad    VARCHAR(50),
    posicion        VARCHAR(50),
    dorsal          INT,
    peso_kg         NUMERIC(5,1),
    altura_cm       NUMERIC(5,1),
    pie_dominante   VARCHAR(20),
    nombre_club     VARCHAR(100),
    liga            VARCHAR(50),
    pais_club       VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS olap.dim_club (
    club_sk        SERIAL PRIMARY KEY,
    club_id        INT,
    nombre_club    VARCHAR(100),
    pais           VARCHAR(50),
    ciudad         VARCHAR(50),
    liga           VARCHAR(50),
    num_jugadores  INT,
    fecha_contrato DATE
);

CREATE TABLE IF NOT EXISTS olap.dim_partido (
    partido_sk      SERIAL PRIMARY KEY,
    partido_id      INT,
    competicion     VARCHAR(50),
    jornada         VARCHAR(50),
    local_visitante VARCHAR(20),
    duracion_min    INT,
    temporada       VARCHAR(10),
    fecha           DATE,
    nombre_club     VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS olap.dim_entrenamiento (
    entrenamiento_sk SERIAL PRIMARY KEY,
    entrenamiento_id INT,
    tipo             VARCHAR(50),
    duracion_min     INT,
    intensidad       INT,
    temporada        VARCHAR(10),
    fecha            DATE,
    nombre_club      VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS olap.fact_rendimiento (
    rendimiento_sk   SERIAL PRIMARY KEY,
    tiempo_sk        INT REFERENCES olap.dim_tiempo(tiempo_sk),
    jugador_sk       INT REFERENCES olap.dim_jugador(jugador_sk),
    club_sk          INT REFERENCES olap.dim_club(club_sk),
    partido_sk       INT REFERENCES olap.dim_partido(partido_sk),
    entrenamiento_sk INT REFERENCES olap.dim_entrenamiento(entrenamiento_sk),
    minuto           INT,
    km_acumulados    NUMERIC(5,2),
    velocidad        NUMERIC(5,1),
    num_sprints      INT,
    aceleraciones    INT,
    frenadas         INT
);


-- ======================================================================
-- PARTE 3 — STAGING (nuevo en Práctica 2)
-- ======================================================================

CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS staging.stg_jugadores_clean (
    id_jugador      INT PRIMARY KEY,
    nombre          VARCHAR(100),
    apellidos       VARCHAR(100),
    nombre_completo VARCHAR(200),
    edad            INT,
    nacionalidad    VARCHAR(60),
    posicion        VARCHAR(40),
    dorsal          INT,
    peso_kg         DECIMAL(5,2),
    altura_cm       DECIMAL(5,2),
    pie_dominante   VARCHAR(10),
    estado          VARCHAR(20),
    nombre_club     VARCHAR(100),
    liga            VARCHAR(80),
    pais_club       VARCHAR(60)
);

CREATE TABLE IF NOT EXISTS staging.resultados_analisis (
    posicion               VARCHAR(40),
    km_medio               DECIMAL(5,2),
    velocidad_media        DECIMAL(5,2),
    objetivo_km            DECIMAL(5,2),
    objetivo_velocidad     DECIMAL(5,2),
    pct_km_cumplido        DECIMAL(6,2),
    pct_velocidad_cumplida DECIMAL(6,2)
);


-- ======================================================================
-- PARTE 4 — ETL EN SQL (equivalente al flujo Pentaho de la Práctica 2)
-- ======================================================================

-- ETL Hito 3: Extraer jugadores activos del OLTP y cargar en Staging
INSERT INTO staging.stg_jugadores_clean (
    id_jugador, nombre, apellidos, nombre_completo,
    edad, nacionalidad, posicion, dorsal,
    peso_kg, altura_cm, pie_dominante, estado,
    nombre_club, liga, pais_club
)
SELECT
    j.id_jugador,
    INITCAP(j.nombre),
    INITCAP(j.apellidos),
    INITCAP(j.nombre) || ' ' || INITCAP(j.apellidos),
    DATE_PART('year', AGE(j.fecha_nacimiento))::INT,
    j.nacionalidad,
    j.posicion,
    j.dorsal,
    j.peso_kg,
    j.altura_cm,
    j.pie_dominante,
    j.estado,
    c.nombre_club,
    c.liga,
    c.pais
FROM oltp_jugadores.jugadores j
JOIN oltp_clubes.clubes c ON j.id_club = c.id_club
WHERE j.estado = 'activo'
ON CONFLICT (id_jugador) DO UPDATE SET
    nombre          = EXCLUDED.nombre,
    apellidos       = EXCLUDED.apellidos,
    nombre_completo = EXCLUDED.nombre_completo,
    edad            = EXCLUDED.edad,
    posicion        = EXCLUDED.posicion,
    estado          = EXCLUDED.estado;


-- ETL Hito 4: Análisis de rendimiento cruzado con objetivos externos
-- (equivalente a analisis_rendimiento_atletadata.ktr)

-- Paso 1: Limpiar tabla antes de cargar
DELETE FROM staging.resultados_analisis;

-- Paso 2: Insertar análisis cruzando OLAP con objetivos por posicion
INSERT INTO staging.resultados_analisis (
    posicion,
    km_medio,
    velocidad_media,
    objetivo_km,
    objetivo_velocidad,
    pct_km_cumplido,
    pct_velocidad_cumplida
)
WITH medias AS (
    SELECT
        dj.posicion,
        ROUND(AVG(fr.km_acumulados)::NUMERIC, 2) AS km_medio,
        ROUND(AVG(fr.velocidad)::NUMERIC, 2)     AS velocidad_media
    FROM olap.fact_rendimiento fr
    JOIN olap.dim_jugador dj ON fr.jugador_sk = dj.jugador_sk
    GROUP BY dj.posicion
),
objetivos (posicion, objetivo_km, objetivo_velocidad) AS (
    VALUES
        ('Portero',        5.0,  55.0),
        ('Defensa',        9.0,  65.0),
        ('Centrocampista', 11.0, 68.0),
        ('Delantero',      10.0, 70.0)
)
SELECT
    m.posicion,
    m.km_medio,
    m.velocidad_media,
    o.objetivo_km,
    o.objetivo_velocidad,
    ROUND((m.km_medio       / o.objetivo_km)        * 100, 2) AS pct_km_cumplido,
    ROUND((m.velocidad_media / o.objetivo_velocidad) * 100, 2) AS pct_velocidad_cumplida
FROM medias m
JOIN objetivos o ON m.posicion = o.posicion
ORDER BY m.km_medio DESC;

-- Verificacion final
SELECT * FROM staging.resultados_analisis;
