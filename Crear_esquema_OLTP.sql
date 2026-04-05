
-- Crear esquema OLTP 
CREATE SCHEMA oltp_clubes;
CREATE SCHEMA oltp_jugadores;
CREATE SCHEMA oltp_partidos;
CREATE SCHEMA oltp_enrenamientos;
create SCHEMA oltp_sensor;
CREATE SCHEMA oltp_medico;

--======================================================================
--======================================================================
-- Crear tabla de clubes
CREATE TABLE oltp_clubes.clubes (
    id_club        SERIAL PRIMARY KEY,
    nombre_club    VARCHAR(100) NOT NULL,
    pais           VARCHAR(60)  NOT NULL,
    ciudad         VARCHAR(60)  NOT NULL,
    liga           VARCHAR(80)  NOT NULL,
    num_jugadores  INTEGER      NOT NULL,
    fecha_contrato DATE         NOT NULL,
    activo         BOOLEAN      DEFAULT TRUE
);
--======================================================================
--======================================================================


-- Crear tabla de jugadores
CREATE TABLE oltp_jugadores.jugadores (
    id_jugador       SERIAL PRIMARY KEY,
    id_club          INTEGER      NOT NULL 
                     REFERENCES oltp_clubes.clubes(id_club),
    nombre           VARCHAR(60)  NOT NULL,
    apellidos        VARCHAR(80)  NOT NULL,
    fecha_nacimiento DATE         NOT NULL,
    nacionalidad     VARCHAR(60)  NOT NULL,
    posicion         VARCHAR(40)  NOT NULL,
    dorsal           INTEGER      NOT NULL,
    peso_kg          DECIMAL(5,2),
    altura_cm        DECIMAL(5,2),
    pie_dominante    VARCHAR(10)  CHECK (pie_dominante IN ('derecho','izquierdo','ambos')),
    estado           VARCHAR(20)  DEFAULT 'activo'
                                  CHECK (estado IN ('activo','lesionado','baja')),
    foto_ruta        VARCHAR(200)
);
--======================================================================
--======================================================================


-- Crear tabla de partidos
CREATE TABLE oltp_partidos.partidos (
    id_partido      SERIAL PRIMARY KEY,
    id_club         INTEGER      NOT NULL 
                    REFERENCES oltp_clubes.clubes(id_club),
    fecha_partido   TIMESTAMP    NOT NULL,
    competicion     VARCHAR(80)  NOT NULL,
    jornada         VARCHAR(40),
    local_visitante VARCHAR(10)  NOT NULL
                    CHECK (local_visitante IN ('local','visitante')),
    duracion_min    INTEGER      DEFAULT 90,
    temporada       VARCHAR(20)  NOT NULL,
    estado          VARCHAR(20)  DEFAULT 'jugado'
                    CHECK (estado IN ('jugado','suspendido','pendiente'))
);

-- Tabla alineaciones: quién juega de titular en cada partido
CREATE TABLE oltp_partidos.alineaciones (
    id_alineacion   SERIAL PRIMARY KEY,
    id_partido      INTEGER NOT NULL REFERENCES oltp_partidos.partidos(id_partido),
    id_jugador      INTEGER NOT NULL REFERENCES oltp_jugadores.jugadores(id_jugador),
    titular         BOOLEAN NOT NULL DEFAULT TRUE,
    posicion_juego  VARCHAR(40) NOT NULL,
    minuto_entrada  INTEGER DEFAULT 0,
    minuto_salida   INTEGER DEFAULT 90
);

-- Tabla cambios: sustituciones durante el partido
CREATE TABLE oltp_partidos.cambios (
    id_cambio       SERIAL PRIMARY KEY,
    id_partido      INTEGER NOT NULL REFERENCES oltp_partidos.partidos(id_partido),
    id_jugador_sale INTEGER NOT NULL REFERENCES oltp_jugadores.jugadores(id_jugador),
    id_jugador_entra INTEGER NOT NULL REFERENCES oltp_jugadores.jugadores(id_jugador),
    minuto          INTEGER NOT NULL
);

-- Tabla tarjetas: tarjetas amarillas y rojas
CREATE TABLE oltp_partidos.tarjetas (
    id_tarjeta      SERIAL PRIMARY KEY,
    id_partido      INTEGER NOT NULL REFERENCES oltp_partidos.partidos(id_partido),
    id_jugador      INTEGER NOT NULL REFERENCES oltp_jugadores.jugadores(id_jugador),
    tipo            VARCHAR(10) NOT NULL CHECK (tipo IN ('amarilla','roja')),
    minuto          INTEGER NOT NULL
);
--======================================================================
--======================================================================


-- Crear tabla de entrenamientos
CREATE TABLE oltp_entrenamientos.entrenamientos (
    id_entrenamiento  SERIAL PRIMARY KEY,
    id_club           INTEGER      NOT NULL 
                      REFERENCES oltp_clubes.clubes(id_club),
    fecha_sesion      TIMESTAMP    NOT NULL,
    tipo_entrenamiento VARCHAR(40) NOT NULL
                       CHECK (tipo_entrenamiento IN (
                           'fisico','tactico','recuperacion',
                           'tecnico','mixto')),
    duracion_min      INTEGER      NOT NULL,
    intensidad        INTEGER      NOT NULL
                       CHECK (intensidad BETWEEN 1 AND 10),
    temporada         VARCHAR(20)  NOT NULL,
    observaciones     TEXT,
    estado            VARCHAR(20)  DEFAULT 'completado'
                       CHECK (estado IN ('completado','cancelado','pendiente'))
);
--======================================================================
--======================================================================


-- Crear tablas de sensores para salud y rendimiento
-- Tabla 1: sensor_salud
CREATE TABLE oltp_sensor.sensor_salud (
    id_registro      SERIAL PRIMARY KEY,
    id_jugador       INTEGER      NOT NULL 
                     REFERENCES oltp_jugadores.jugadores(id_jugador),

    id_partido       INTEGER      REFERENCES oltp_partidos.partidos(id_partido),

    id_entrenamiento INTEGER      
                     REFERENCES oltp_entrenamientos.entrenamientos(id_entrenamiento),

    minuto           INTEGER      NOT NULL CHECK (minuto >= 0),
    frec_cardiaca    INTEGER      NOT NULL,
    temperatura      DECIMAL(4,1) NOT NULL,
    fatiga           DECIMAL(4,2) NOT NULL CHECK (fatiga BETWEEN 0 AND 10),
    equilibrio       DECIMAL(5,2),
    riesgo_lesion    DECIMAL(4,2) CHECK (riesgo_lesion BETWEEN 0 AND 100),
    timestamp_real   TIMESTAMP    DEFAULT NOW()
);
 
-- Tabla 2: sensor_rendimiento
CREATE TABLE oltp_sensor.sensor_rendimiento (
    id_registro      SERIAL PRIMARY KEY,
    id_jugador       INTEGER      NOT NULL 
                     REFERENCES oltp_jugadores.jugadores(id_jugador),

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
--======================================================================
--======================================================================

-- Crear tabla de historial médico
CREATE TABLE oltp_medico.historial_medico (
    id_lesion           SERIAL PRIMARY KEY,
    id_jugador          INTEGER      NOT NULL 
                        REFERENCES oltp_jugadores.jugadores(id_jugador),
    tipo_lesion         VARCHAR(80)  NOT NULL,
    zona_cuerpo         VARCHAR(60)  NOT NULL,
    gravedad            VARCHAR(20)  NOT NULL
                        CHECK (gravedad IN ('leve','moderada','grave')),
    fecha_inicio        DATE         NOT NULL,
    fecha_alta          DATE,
    dias_descanso       INTEGER      NOT NULL,
    tratamiento         TEXT,
    medico_responsable  VARCHAR(100) NOT NULL,
    detectada_sensor    BOOLEAN      DEFAULT FALSE,
    observaciones       TEXT
);

-- Tabla bajas: jugadores dados de baja por el médico
CREATE TABLE oltp_medico.bajas (
    id_baja         SERIAL PRIMARY KEY,
    id_jugador      INTEGER NOT NULL REFERENCES oltp_jugadores.jugadores(id_jugador),
    fecha_inicio    DATE NOT NULL,
    fecha_fin       DATE,
    motivo          VARCHAR(100) NOT NULL,
    medico          VARCHAR(100) NOT NULL,
    activa          BOOLEAN DEFAULT TRUE
);
--======================================================================
--======================================================================