import psycopg2
import time
import statistics

# ─── CONEXION ───
conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="AtletaData",
    user="postgres",
    password="M*******4"
)
cur = conn.cursor()
print("Conectado correctamente\n")

# ─── CONSULTAS OLTP ───
consultas_oltp = {
    "Q1 Fatiga por minuto": """
        SELECT j.nombre || ' ' || j.apellidos AS jugador,
               c.nombre_club, ss.minuto,
               AVG(ss.fatiga) AS fatiga_media
        FROM oltp_sensor.sensor_salud ss
        JOIN oltp_jugadores.jugadores j ON ss.id_jugador = j.id_jugador
        JOIN oltp_clubes.clubes c ON j.id_club = c.id_club
        JOIN oltp_partidos.partidos p ON ss.id_partido = p.id_partido
        GROUP BY j.nombre, j.apellidos, c.nombre_club, ss.minuto
        ORDER BY jugador, ss.minuto
    """,
    "Q2 Riesgo lesion semanal": """
        SELECT j.nombre || ' ' || j.apellidos AS jugador,
               c.nombre_club,
               EXTRACT(WEEK FROM COALESCE(
                   p.fecha_partido, e.fecha_sesion))::INT AS semana,
               AVG(ss.riesgo_lesion) AS riesgo_medio,
               MAX(ss.riesgo_lesion) AS riesgo_maximo,
               AVG(ss.fatiga) AS fatiga_media,
               COUNT(CASE WHEN ss.riesgo_lesion > 70 THEN 1 END) AS num_alertas
        FROM oltp_sensor.sensor_salud ss
        JOIN oltp_jugadores.jugadores j ON ss.id_jugador = j.id_jugador
        JOIN oltp_clubes.clubes c ON j.id_club = c.id_club
        LEFT JOIN oltp_partidos.partidos p ON ss.id_partido = p.id_partido
        LEFT JOIN oltp_entrenamientos.entrenamientos e 
            ON ss.id_entrenamiento = e.id_entrenamiento
        GROUP BY j.nombre, j.apellidos, c.nombre_club, semana
        ORDER BY riesgo_medio DESC
        LIMIT 10
    """,
    "Q3 Evolucion salud mensual": """
        SELECT j.nombre || ' ' || j.apellidos AS jugador,
               EXTRACT(MONTH FROM COALESCE(
                   p.fecha_partido, e.fecha_sesion))::INT AS mes,
               AVG(ss.frec_cardiaca) AS fc_media,
               AVG(ss.temperatura)   AS temp_media,
               AVG(ss.fatiga)        AS fatiga_media
        FROM oltp_sensor.sensor_salud ss
        JOIN oltp_jugadores.jugadores j ON ss.id_jugador = j.id_jugador
        JOIN oltp_clubes.clubes c ON j.id_club = c.id_club
        LEFT JOIN oltp_partidos.partidos p ON ss.id_partido = p.id_partido
        LEFT JOIN oltp_entrenamientos.entrenamientos e 
            ON ss.id_entrenamiento = e.id_entrenamiento
        GROUP BY j.nombre, j.apellidos, c.nombre_club, mes
        ORDER BY jugador, mes
    """,
    "Q4 Velocidad maxima mensual": """
        SELECT j.nombre || ' ' || j.apellidos AS jugador,
               j.posicion, c.nombre_club,
               EXTRACT(MONTH FROM COALESCE(
                   p.fecha_partido, e.fecha_sesion))::INT AS mes,
               MAX(sr.velocidad)     AS velocidad_maxima,
               AVG(sr.velocidad)     AS velocidad_media
        FROM oltp_sensor.sensor_rendimiento sr
        JOIN oltp_jugadores.jugadores j ON sr.id_jugador = j.id_jugador
        JOIN oltp_clubes.clubes c ON j.id_club = c.id_club
        LEFT JOIN oltp_partidos.partidos p ON sr.id_partido = p.id_partido
        LEFT JOIN oltp_entrenamientos.entrenamientos e 
            ON sr.id_entrenamiento = e.id_entrenamiento
        GROUP BY j.nombre, j.apellidos, j.posicion, c.nombre_club, mes
        ORDER BY velocidad_maxima DESC
    """,
    "Q5 Desequilibrio corporal": """
        SELECT j.nombre || ' ' || j.apellidos AS jugador,
               j.posicion, c.nombre_club,
               AVG(ss.equilibrio)    AS desequilibrio_medio,
               MAX(ss.equilibrio)    AS desequilibrio_maximo,
               AVG(ss.riesgo_lesion) AS riesgo_medio,
               COUNT(CASE WHEN ss.equilibrio > 0.20 THEN 1 END) AS episodios
        FROM oltp_sensor.sensor_salud ss
        JOIN oltp_jugadores.jugadores j ON ss.id_jugador = j.id_jugador
        JOIN oltp_clubes.clubes c ON j.id_club = c.id_club
        GROUP BY j.nombre, j.apellidos, j.posicion, c.nombre_club
        ORDER BY desequilibrio_medio DESC
    """
}

# ─── CONSULTAS OLAP ───
consultas_olap = {
    "Q1 Fatiga por minuto": """
        SELECT dj.nombre_completo AS jugador,
               dj.nombre_club, fs.minuto,
               AVG(fs.fatiga) AS fatiga_media
        FROM olap.fact_salud fs
        JOIN olap.dim_jugador dj ON fs.jugador_sk = dj.jugador_sk
        WHERE fs.partido_sk IS NOT NULL
        GROUP BY dj.nombre_completo, dj.nombre_club, fs.minuto
        ORDER BY jugador, fs.minuto
    """,
    "Q2 Riesgo lesion semanal": """
        SELECT dj.nombre_completo AS jugador,
               dj.nombre_club, fl.semana,
               fl.riesgo_medio, fl.riesgo_maximo,
               fl.fatiga_media, fl.num_alertas
        FROM olap.fact_lesion_riesgo fl
        JOIN olap.dim_jugador dj ON fl.jugador_sk = dj.jugador_sk
        ORDER BY fl.riesgo_medio DESC
        LIMIT 10
    """,
    "Q3 Evolucion salud mensual": """
        SELECT dj.nombre_completo AS jugador,
               dj.nombre_club, dt.nombre_mes, dt.mes,
               AVG(fs.frec_cardiaca) AS fc_media,
               AVG(fs.temperatura)   AS temp_media,
               AVG(fs.fatiga)        AS fatiga_media
        FROM olap.fact_salud fs
        JOIN olap.dim_jugador dj ON fs.jugador_sk = dj.jugador_sk
        JOIN olap.dim_tiempo dt   ON fs.tiempo_sk = dt.tiempo_sk
        GROUP BY dj.nombre_completo, dj.nombre_club, dt.nombre_mes, dt.mes
        ORDER BY jugador, dt.mes
    """,
    "Q4 Velocidad maxima mensual": """
        SELECT dj.nombre_completo AS jugador,
               dj.posicion, dj.nombre_club,
               dt.nombre_mes, dt.mes,
               MAX(fr.velocidad)    AS velocidad_maxima,
               AVG(fr.velocidad)    AS velocidad_media
        FROM olap.fact_rendimiento fr
        JOIN olap.dim_jugador dj ON fr.jugador_sk = dj.jugador_sk
        JOIN olap.dim_tiempo dt  ON fr.tiempo_sk = dt.tiempo_sk
        GROUP BY dj.nombre_completo, dj.posicion,
                 dj.nombre_club, dt.nombre_mes, dt.mes
        ORDER BY velocidad_maxima DESC
    """,
    "Q5 Desequilibrio corporal": """
        SELECT dj.nombre_completo AS jugador,
               dj.posicion, dj.nombre_club,
               AVG(fl.desequilibrio_medio) AS desequilibrio_medio,
               MAX(fl.desequilibrio_medio) AS desequilibrio_maximo,
               AVG(fl.riesgo_medio)        AS riesgo_medio,
               SUM(fl.num_alertas)         AS total_alertas
        FROM olap.fact_lesion_riesgo fl
        JOIN olap.dim_jugador dj ON fl.jugador_sk = dj.jugador_sk
        GROUP BY dj.nombre_completo, dj.posicion, dj.nombre_club
        ORDER BY desequilibrio_medio DESC
    """
}

# ─── FUNCION DE MEDICION ───
def medir(query, repeticiones=10):
    tiempos = []
    for _ in range(repeticiones):
        inicio = time.time()
        cur.execute(query)
        cur.fetchall()
        fin = time.time()
        tiempos.append((fin - inicio) * 1000)  # en milisegundos
    return {
        "media":   round(statistics.mean(tiempos), 2),
        "minimo":  round(min(tiempos), 2),
        "maximo":  round(max(tiempos), 2),
        "desv":    round(statistics.stdev(tiempos), 2)
    }

# ─── EJECUCION ───
resultados = {}

print("Midiendo consultas OLTP...")
for nombre, query in consultas_oltp.items():
    print(f"  {nombre}...")
    resultados[nombre] = {"oltp": medir(query)}

print("\nMidiendo consultas OLAP...")
for nombre, query in consultas_olap.items():
    print(f"  {nombre}...")
    resultados[nombre]["olap"] = medir(query)

# ─── RESULTADOS ───
print("\n" + "="*75)
print(f"{'Consulta':<30} {'OLTP(ms)':>10} {'OLAP(ms)':>10} {'Mejora':>10}")
print("="*75)

for nombre, datos in resultados.items():
    oltp_ms = datos["oltp"]["media"]
    olap_ms = datos["olap"]["media"]
    if olap_ms > 0:
        mejora = round(((oltp_ms - olap_ms) / oltp_ms) * 100, 1)
    else:
        mejora = 0
    signo = "+" if mejora > 0 else ""
    print(f"{nombre:<30} {oltp_ms:>10} {olap_ms:>10} {signo}{mejora:>9}%")

print("="*75)
print("\nDetalle por consulta:")
for nombre, datos in resultados.items():
    print(f"\n{nombre}")
    print(f"  OLTP — media: {datos['oltp']['media']}ms  "
          f"min: {datos['oltp']['minimo']}ms  "
          f"max: {datos['oltp']['maximo']}ms  "
          f"desv: {datos['oltp']['desv']}ms")
    print(f"  OLAP — media: {datos['olap']['media']}ms  "
          f"min: {datos['olap']['minimo']}ms  "
          f"max: {datos['olap']['maximo']}ms  "
          f"desv: {datos['olap']['desv']}ms")

cur.close()
conn.close()
print("\nBenchmark completado")