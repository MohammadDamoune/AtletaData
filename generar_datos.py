import psycopg2
import random
from datetime import date, timedelta, datetime

# CONEXION - pon tu contraseña aqui
conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="AtletaData",
    user="postgres",
    password="M******4"
)
cur = conn.cursor()

print("Conectado a PostgreSQL correctamente")

# ============================================================
# 1. CLUBES
# ============================================================
clubes = [
    ("Real Madrid CF",    "España", "Madrid",    "La Liga", 25, "2023-07-01"),
    ("FC Barcelona",      "España", "Barcelona", "La Liga", 25, "2023-07-01"),
    ("Atletico de Madrid","España", "Madrid",    "La Liga", 25, "2023-08-01"),
]

for c in clubes:
    cur.execute("""
        INSERT INTO oltp_clubes.clubes 
        (nombre_club, pais, ciudad, liga, num_jugadores, fecha_contrato, activo)
        VALUES (%s, %s, %s, %s, %s, %s, TRUE)
    """, c)

conn.commit()
print("Clubes insertados")

# ============================================================
# 2. JUGADORES - 25 por club
# ============================================================
jugadores_real_madrid = [
    ("Thibaut",    "Courtois",        "1992-05-11", "Belga",     "Portero",        1,  90.0, 199.0, "derecho"),
    ("Daniel",     "Carvajal",        "1992-01-11", "Española",  "Defensa",        2,  73.0, 173.0, "derecho"),
    ("Eder",       "Militao",         "1998-01-18", "Brasileña", "Defensa",        3,  78.0, 186.0, "derecho"),
    ("Antonio",    "Rudiger",         "1993-03-03", "Alemana",   "Defensa",        22, 85.0, 190.0, "derecho"),
    ("Ferland",    "Mendy",           "1995-06-08", "Francesa",  "Defensa",        23, 74.0, 180.0, "izquierdo"),
    ("Aurelien",   "Tchouameni",      "2000-01-16", "Francesa",  "Centrocampista", 18, 78.0, 187.0, "derecho"),
    ("Luka",       "Modric",          "1985-09-09", "Croata",    "Centrocampista", 10, 66.0, 172.0, "derecho"),
    ("Eduardo",    "Camavinga",       "2002-11-10", "Francesa",  "Centrocampista", 12, 73.0, 181.0, "izquierdo"),
    ("Federico",   "Valverde",        "1998-07-22", "Uruguaya",  "Centrocampista", 15, 75.0, 182.0, "derecho"),
    ("Jude",       "Bellingham",      "2003-06-29", "Inglesa",   "Centrocampista", 5,  75.0, 186.0, "derecho"),
    ("Vinicius",   "Junior",          "2000-07-12", "Brasileña", "Delantero",      7,  73.0, 176.0, "derecho"),
    ("Rodrygo",    "Goes",            "2001-01-09", "Brasileña", "Delantero",      11, 68.0, 174.0, "derecho"),
    ("Kylian",     "Mbappe",          "1998-12-20", "Francesa",  "Delantero",      9,  73.0, 178.0, "derecho"),
    ("Dani",       "Ceballos",        "1996-08-07", "Española",  "Centrocampista", 19, 68.0, 176.0, "derecho"),
    ("Marco",      "Asensio",         "1996-01-21", "Española",  "Delantero",      20, 70.0, 182.0, "derecho"),
    ("Brahim",     "Diaz",            "1999-08-03", "Española",  "Delantero",      21, 62.0, 170.0, "derecho"),
    ("Lucas",      "Vazquez",         "1991-07-01", "Española",  "Defensa",        17, 70.0, 173.0, "derecho"),
    ("Nacho",      "Fernandez",       "1990-01-18", "Española",  "Defensa",        6,  78.0, 180.0, "derecho"),
    ("Kepa",       "Arrizabalaga",    "1994-10-03", "Española",  "Portero",        13, 84.0, 186.0, "derecho"),
    ("Jesus",      "Vallejo",         "1997-01-05", "Española",  "Defensa",        8,  76.0, 182.0, "derecho"),
    ("Sergio",     "Arribas",         "2001-11-23", "Española",  "Delantero",      16, 67.0, 176.0, "derecho"),
    ("Mario",      "Martin",          "2003-04-15", "Española",  "Centrocampista", 24, 70.0, 178.0, "derecho"),
    ("Pablo",      "Ramon",           "2003-02-08", "Española",  "Defensa",        25, 75.0, 183.0, "derecho"),
    ("Carlos",     "Dotor",           "2000-09-21", "Española",  "Centrocampista", 14, 72.0, 179.0, "derecho"),
    ("Antonio",    "Blanco",          "2000-12-16", "Española",  "Centrocampista", 4,  71.0, 180.0, "derecho"),
]

jugadores_barcelona = [
    ("Marc",       "ter Stegen",      "1992-04-30", "Alemana",   "Portero",        1,  85.0, 187.0, "derecho"),
    ("Jules",      "Kounde",          "1998-11-12", "Francesa",  "Defensa",        23, 70.0, 178.0, "derecho"),
    ("Ronald",     "Araujo",          "1999-03-07", "Uruguaya",  "Defensa",        4,  82.0, 188.0, "derecho"),
    ("Inigo",      "Martinez",        "1991-05-17", "Española",  "Defensa",        5,  80.0, 182.0, "izquierdo"),
    ("Alejandro",  "Balde",           "2003-10-18", "Española",  "Defensa",        3,  65.0, 177.0, "izquierdo"),
    ("Frenkie",    "de Jong",         "1997-05-12", "Holandesa", "Centrocampista", 21, 74.0, 180.0, "derecho"),
    ("Pedri",      "Gonzalez",        "2002-11-25", "Española",  "Centrocampista", 8,  60.0, 174.0, "derecho"),
    ("Gavi",       "Paez",            "2004-08-05", "Española",  "Centrocampista", 6,  60.0, 173.0, "derecho"),
    ("Ilkay",      "Gundogan",        "1990-10-24", "Alemana",   "Centrocampista", 22, 80.0, 182.0, "derecho"),
    ("Robert",     "Lewandowski",     "1988-08-21", "Polaca",    "Delantero",      9,  79.0, 185.0, "derecho"),
    ("Raphinha",   "Belloli",         "1996-12-14", "Brasileña", "Delantero",      11, 68.0, 176.0, "derecho"),
    ("Ferran",     "Torres",          "2000-02-29", "Española",  "Delantero",      7,  69.0, 181.0, "derecho"),
    ("Lamine",     "Yamal",           "2007-07-13", "Española",  "Delantero",      27, 60.0, 176.0, "derecho"),
    ("Andreas",    "Christensen",     "1996-04-10", "Danesa",    "Defensa",        15, 78.0, 187.0, "derecho"),
    ("Eric",       "Garcia",          "2001-01-09", "Española",  "Defensa",        24, 70.0, 182.0, "derecho"),
    ("Sergi",      "Roberto",         "1992-02-07", "Española",  "Defensa",        20, 68.0, 178.0, "derecho"),
    ("Oriol",      "Romeu",           "1991-09-24", "Española",  "Centrocampista", 16, 80.0, 181.0, "derecho"),
    ("Vitor",      "Roque",           "2005-02-28", "Brasileña", "Delantero",      19, 70.0, 174.0, "derecho"),
    ("Inaki",      "Pena",            "1999-03-02", "Española",  "Portero",        13, 82.0, 189.0, "derecho"),
    ("Ansu",       "Fati",            "2002-10-31", "Española",  "Delantero",      10, 60.0, 177.0, "izquierdo"),
    ("Pablo",      "Torre",           "2003-06-13", "Española",  "Centrocampista", 17, 65.0, 178.0, "derecho"),
    ("Marc",       "Casado",          "2003-01-01", "Española",  "Centrocampista", 18, 68.0, 180.0, "derecho"),
    ("Hector",     "Fort",            "2006-02-15", "Española",  "Defensa",        25, 72.0, 181.0, "derecho"),
    ("Pau",        "Cubarsi",         "2007-01-22", "Española",  "Defensa",        2,  75.0, 185.0, "derecho"),
    ("Dani",       "Olmo",            "1998-05-07", "Española",  "Centrocampista", 20, 70.0, 179.0, "derecho"),
]

jugadores_atletico = [
    ("Jan",        "Oblak",           "1993-01-07", "Eslovena",  "Portero",        13, 87.0, 188.0, "derecho"),
    ("Nahuel",     "Molina",          "1998-04-06", "Argentina", "Defensa",        16, 72.0, 176.0, "derecho"),
    ("Jose",       "Gimenez",         "1995-01-20", "Uruguaya",  "Defensa",        2,  82.0, 187.0, "derecho"),
    ("Cesar",      "Azpilicueta",     "1989-08-28", "Española",  "Defensa",        17, 76.0, 176.0, "derecho"),
    ("Reinildo",   "Mandava",         "1994-01-21", "Mozambicana","Defensa",       3,  72.0, 179.0, "izquierdo"),
    ("Koke",       "Resurreccion",    "1992-01-08", "Española",  "Centrocampista", 6,  68.0, 176.0, "derecho"),
    ("Rodrigo",    "De Paul",         "1994-05-24", "Argentina", "Centrocampista", 5,  75.0, 180.0, "derecho"),
    ("Saul",       "Niguez",          "1994-11-21", "Española",  "Centrocampista", 8,  76.0, 184.0, "derecho"),
    ("Pablo",      "Barrios",         "2003-03-06", "Española",  "Centrocampista", 20, 70.0, 181.0, "derecho"),
    ("Antoine",    "Griezmann",       "1991-03-21", "Francesa",  "Delantero",      7,  73.0, 176.0, "derecho"),
    ("Alvaro",     "Morata",          "1992-10-23", "Española",  "Delantero",      9,  79.0, 187.0, "derecho"),
    ("Samuel",     "Lino",            "1999-11-20", "Española",  "Delantero",      11, 66.0, 172.0, "izquierdo"),
    ("Angel",      "Correa",          "1994-03-09", "Argentina", "Delantero",      10, 68.0, 174.0, "derecho"),
    ("Stefan",     "Savic",           "1991-01-08", "Montenegrina","Defensa",      15, 83.0, 189.0, "derecho"),
    ("Marcos",     "Llorente",        "1995-01-30", "Española",  "Centrocampista", 14, 78.0, 184.0, "derecho"),
    ("Thomas",     "Lemar",           "1995-11-12", "Francesa",  "Centrocampista", 11, 68.0, 175.0, "izquierdo"),
    ("Axel",       "Witsel",          "1989-01-12", "Belga",     "Centrocampista", 20, 81.0, 186.0, "derecho"),
    ("Memphis",    "Depay",           "1994-02-13", "Holandesa", "Delantero",      9,  78.0, 176.0, "derecho"),
    ("Ivo",        "Grbic",           "1996-01-18", "Croata",    "Portero",        25, 86.0, 194.0, "derecho"),
    ("Felipe",     "Monteiro",        "1989-05-16", "Brasileña", "Defensa",        4,  83.0, 186.0, "derecho"),
    ("Sime",       "Vrsaljko",        "1992-01-10", "Croata",    "Defensa",        24, 75.0, 180.0, "derecho"),
    ("Javi",       "Serrano",         "2003-07-01", "Española",  "Defensa",        23, 70.0, 179.0, "derecho"),
    ("Adrian",     "Niño",            "2004-02-14", "Española",  "Delantero",      22, 65.0, 175.0, "derecho"),
    ("Giuliano",   "Simeone",         "2003-07-05", "Argentina", "Delantero",      21, 68.0, 177.0, "derecho"),
    ("Rodrigo",    "Riquelme",        "1999-04-07", "Española",  "Delantero",      18, 67.0, 175.0, "izquierdo"),
]

todos_jugadores = [
    (1, jugadores_real_madrid),
    (2, jugadores_barcelona),
    (3, jugadores_atletico),
]

for id_club, jugadores in todos_jugadores:
    for j in jugadores:
        cur.execute("""
            INSERT INTO oltp_jugadores.jugadores
            (id_club, nombre, apellidos, fecha_nacimiento, nacionalidad,
             posicion, dorsal, peso_kg, altura_cm, pie_dominante, estado)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'activo')
        """, (id_club, j[0], j[1], j[2], j[3], j[4], j[5], j[6], j[7], j[8]))

conn.commit()
print("Jugadores insertados")

# ============================================================
# 3. PARTIDOS - 10 por club
# ============================================================
partidos_data = []
fecha_base = date(2024, 9, 1)

rivales = [
    "Sevilla FC", "Valencia CF", "Villarreal CF", "Real Sociedad",
    "Athletic Club", "Real Betis", "Getafe CF", "Girona FC",
    "Osasuna", "Rayo Vallecano"
]

for id_club in range(1, 4):
    for i in range(10):
        fecha = fecha_base + timedelta(weeks=i*2)
        local_vis = "local" if i % 2 == 0 else "visitante"
        partidos_data.append((
            id_club,
            datetime.combine(fecha, datetime.min.time()).replace(hour=20),
            "La Liga",
            f"Jornada {i+1}",
            local_vis,
            90,
            "2024-25",
            "jugado"
        ))

for p in partidos_data:
    cur.execute("""
        INSERT INTO oltp_partidos.partidos
        (id_club, fecha_partido, competicion, jornada,
         local_visitante, duracion_min, temporada, estado)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, p)

conn.commit()
print("Partidos insertados")

# ============================================================
# 4. ENTRENAMIENTOS - 15 por club
# ============================================================
tipos = ["fisico", "tactico", "recuperacion", "tecnico", "mixto"]
fecha_ent = date(2024, 9, 2)

for id_club in range(1, 4):
    for i in range(15):
        fecha = fecha_ent + timedelta(days=i*5)
        tipo = tipos[i % len(tipos)]
        intensidad = random.randint(3, 9)
        cur.execute("""
            INSERT INTO oltp_entrenamientos.entrenamientos
            (id_club, fecha_sesion, tipo_entrenamiento, duracion_min,
             intensidad, temporada, observaciones, estado)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            id_club,
            datetime.combine(fecha, datetime.min.time()).replace(hour=10),
            tipo,
            random.randint(60, 90),
            intensidad,
            "2024-25",
            f"Sesion {tipo} intensidad {intensidad}",
            "completado"
        ))

conn.commit()
print("Entrenamientos insertados")

# ============================================================
# 5. RECUPERAR IDs de jugadores por club
# ============================================================
cur.execute("""
    SELECT id_jugador, id_club, posicion, estado
    FROM oltp_jugadores.jugadores
    ORDER BY id_club, id_jugador
""")
jugadores_db = cur.fetchall()

jugadores_por_club = {1: [], 2: [], 3: []}
for j in jugadores_db:
    jugadores_por_club[j[1]].append({
        "id": j[0],
        "posicion": j[2],
        "estado": j[3]
    })

# ============================================================
# 6. RECUPERAR IDs de partidos y entrenamientos
# ============================================================
cur.execute("SELECT id_partido, id_club FROM oltp_partidos.partidos ORDER BY id_club, id_partido")
partidos_db = cur.fetchall()
partidos_por_club = {1: [], 2: [], 3: []}
for p in partidos_db:
    partidos_por_club[p[1]].append(p[0])

cur.execute("SELECT id_entrenamiento, id_club FROM oltp_entrenamientos.entrenamientos ORDER BY id_club, id_entrenamiento")
ents_db = cur.fetchall()
ents_por_club = {1: [], 2: [], 3: []}
for e in ents_db:
    ents_por_club[e[1]].append(e[0])

# ============================================================
# 7. ALINEACIONES, CAMBIOS, TARJETAS y SENSOR
# ============================================================

def generar_alineacion(jugadores_club):
    """Selecciona 11 titulares y 3 suplentes que entran"""
    disponibles = [j for j in jugadores_club if j["estado"] == "activo"]

    # Intentar poner al menos 1 portero
    porteros = [j for j in disponibles if j["posicion"] == "Portero"]
    no_porteros = [j for j in disponibles if j["posicion"] != "Portero"]

    titulares = []
    if porteros:
        titulares.append(porteros[0])
    
    random.shuffle(no_porteros)
    titulares += no_porteros[:10]

    # Si no hay suficientes cogemos los que hay
    if len(titulares) < 11:
        titulares = disponibles[:11]

    suplentes = [j for j in disponibles if j not in titulares]
    random.shuffle(suplentes)
    entrantes = suplentes[:3]

    return titulares[:11], entrantes

def datos_sensor_salud(id_jugador, minuto_entrada, minuto_salida, id_partido=None, id_entrenamiento=None):
    registros = []
    for minuto in range(minuto_entrada, minuto_salida + 1, 5):
        progreso = (minuto - minuto_entrada) / max((minuto_salida - minuto_entrada), 1)
        fatiga = round(0.3 + progreso * 8.0 + random.uniform(-0.3, 0.3), 2)
        if fatiga < 0: fatiga = 0.0
        if fatiga > 10: fatiga = 10.0
        fc = int(70 + progreso * 110 + random.uniform(-5, 5))
        temp = round(36.5 + progreso * 2.5 + random.uniform(-0.1, 0.1), 1)
        equilibrio = round(0.01 + progreso * 0.29 + random.uniform(-0.01, 0.01), 2)
        riesgo = round(3.0 + progreso * 60.0 + random.uniform(-2, 2), 1)
        if riesgo < 0: riesgo = 0.0
        if riesgo > 100: riesgo = 100.0
        registros.append((
            id_jugador, id_partido, id_entrenamiento,
            minuto, fc, temp, fatiga, equilibrio, riesgo
        ))
    return registros

def datos_sensor_rendimiento(id_jugador, minuto_entrada, minuto_salida, id_partido=None, id_entrenamiento=None):
    registros = []
    km = 0.0
    for minuto in range(minuto_entrada, minuto_salida + 1, 5):
        progreso = (minuto - minuto_entrada) / max((minuto_salida - minuto_entrada), 1)
        vel = round(28.0 - progreso * 14.0 + random.uniform(-2, 2), 1)
        if vel < 5.0: vel = 5.0
        km += round(vel * 5 / 60, 2)
        sprints = int(progreso * 18 + random.uniform(0, 2))
        acels = int(progreso * 35 + random.uniform(0, 3))
        frens = int(progreso * 33 + random.uniform(0, 3))
        registros.append((
            id_jugador, id_partido, id_entrenamiento,
            minuto, round(km, 2), vel, sprints, acels, frens
        ))
    return registros

# Procesar partidos
print("Generando datos de partidos...")
for id_club in range(1, 4):
    jugadores_club = jugadores_por_club[id_club]
    bajas_activas = set()  # ids de jugadores con baja medica

    for id_partido in partidos_por_club[id_club]:
        # Marcar aleatoriamente 1-2 jugadores como lesionados
        disponibles = [j for j in jugadores_club if j["id"] not in bajas_activas]
        if random.random() < 0.4 and len(disponibles) > 14:
            lesionado = random.choice(disponibles)
            bajas_activas.add(lesionado["id"])
            cur.execute("""
                INSERT INTO oltp_medico.bajas
                (id_jugador, fecha_inicio, fecha_fin, motivo, medico, activa)
                VALUES (%s, CURRENT_DATE, CURRENT_DATE + 14, %s, %s, TRUE)
            """, (lesionado["id"], "Lesion muscular", "Dr. Garcia"))

        # Generar alineacion
        titulares, entrantes = generar_alineacion(
            [j for j in jugadores_club if j["id"] not in bajas_activas]
        )

        # Minutos de cambios
        minutos_cambios = sorted(random.sample(range(55, 85), 3))

        # Insertar titulares en alineacion
        for j in titulares:
            cur.execute("""
                INSERT INTO oltp_partidos.alineaciones
                (id_partido, id_jugador, titular, posicion_juego, minuto_entrada, minuto_salida)
                VALUES (%s, %s, TRUE, %s, 0, 90)
            """, (id_partido, j["id"], j["posicion"]))

        # Insertar cambios
        for idx, entrante in enumerate(entrantes):
            saliente = titulares[idx + 8]  # sale un jugador de campo
            min_cambio = minutos_cambios[idx]

            cur.execute("""
                INSERT INTO oltp_partidos.cambios
                (id_partido, id_jugador_sale, id_jugador_entra, minuto)
                VALUES (%s, %s, %s, %s)
            """, (id_partido, saliente["id"], entrante["id"], min_cambio))

            # Actualizar minuto salida del titular
            cur.execute("""
                UPDATE oltp_partidos.alineaciones
                SET minuto_salida = %s
                WHERE id_partido = %s AND id_jugador = %s
            """, (min_cambio, id_partido, saliente["id"]))

            # Insertar suplente en alineacion
            cur.execute("""
                INSERT INTO oltp_partidos.alineaciones
                (id_partido, id_jugador, titular, posicion_juego, minuto_entrada, minuto_salida)
                VALUES (%s, %s, FALSE, %s, %s, 90)
            """, (id_partido, entrante["id"], entrante["posicion"], min_cambio))

        # Tarjeta roja aleatoria (20% probabilidad)
        if random.random() < 0.2:
            jugador_tarjeta = random.choice(titulares)
            min_tarjeta = random.randint(30, 89)
            cur.execute("""
                INSERT INTO oltp_partidos.tarjetas
                (id_partido, id_jugador, tipo, minuto)
                VALUES (%s, %s, 'roja', %s)
            """, (id_partido, jugador_tarjeta["id"], min_tarjeta))
            bajas_activas.add(jugador_tarjeta["id"])

        # Sensor para cada jugador que jugó
        cur.execute("""
            SELECT id_jugador, minuto_entrada, minuto_salida
            FROM oltp_partidos.alineaciones
            WHERE id_partido = %s
        """, (id_partido,))
        alineacion = cur.fetchall()

        for id_jug, min_ent, min_sal in alineacion:
            for reg in datos_sensor_salud(id_jug, min_ent, min_sal, id_partido=id_partido):
                cur.execute("""
                    INSERT INTO oltp_sensor.sensor_salud
                    (id_jugador, id_partido, id_entrenamiento, minuto,
                     frec_cardiaca, temperatura, fatiga, equilibrio, riesgo_lesion)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, reg)

            for reg in datos_sensor_rendimiento(id_jug, min_ent, min_sal, id_partido=id_partido):
                cur.execute("""
                    INSERT INTO oltp_sensor.sensor_rendimiento
                    (id_jugador, id_partido, id_entrenamiento, minuto,
                     km_acumulados, velocidad, num_sprints, aceleraciones, frenadas)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, reg)

    conn.commit()
    print(f"Club {id_club} — partidos procesados")

# Procesar entrenamientos
print("Generando datos de entrenamientos...")
for id_club in range(1, 4):
    jugadores_club = jugadores_por_club[id_club]

    for id_ent in ents_por_club[id_club]:
        # En entrenamientos participan entre 18 y 23 jugadores
        participantes = random.sample(jugadores_club, random.randint(18, 23))

        for j in participantes:
            duracion = random.randint(60, 90)
            for reg in datos_sensor_salud(j["id"], 0, duracion, id_entrenamiento=id_ent):
                cur.execute("""
                    INSERT INTO oltp_sensor.sensor_salud
                    (id_jugador, id_partido, id_entrenamiento, minuto,
                     frec_cardiaca, temperatura, fatiga, equilibrio, riesgo_lesion)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, reg)

            for reg in datos_sensor_rendimiento(j["id"], 0, duracion, id_entrenamiento=id_ent):
                cur.execute("""
                    INSERT INTO oltp_sensor.sensor_rendimiento
                    (id_jugador, id_partido, id_entrenamiento, minuto,
                     km_acumulados, velocidad, num_sprints, aceleraciones, frenadas)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, reg)

    conn.commit()
    print(f"Club {id_club} — entrenamientos procesados")

# ============================================================
# 8. RESUMEN FINAL
# ============================================================
cur.execute("SELECT COUNT(*) FROM oltp_clubes.clubes")
print(f"\nClubes: {cur.fetchone()[0]}")
cur.execute("SELECT COUNT(*) FROM oltp_jugadores.jugadores")
print(f"Jugadores: {cur.fetchone()[0]}")
cur.execute("SELECT COUNT(*) FROM oltp_partidos.partidos")
print(f"Partidos: {cur.fetchone()[0]}")
cur.execute("SELECT COUNT(*) FROM oltp_entrenamientos.entrenamientos")
print(f"Entrenamientos: {cur.fetchone()[0]}")
cur.execute("SELECT COUNT(*) FROM oltp_partidos.alineaciones")
print(f"Alineaciones: {cur.fetchone()[0]}")
cur.execute("SELECT COUNT(*) FROM oltp_partidos.cambios")
print(f"Cambios: {cur.fetchone()[0]}")
cur.execute("SELECT COUNT(*) FROM oltp_partidos.tarjetas")
print(f"Tarjetas: {cur.fetchone()[0]}")
cur.execute("SELECT COUNT(*) FROM oltp_medico.bajas")
print(f"Bajas medicas: {cur.fetchone()[0]}")
cur.execute("SELECT COUNT(*) FROM oltp_sensor.sensor_salud")
print(f"Registros sensor salud: {cur.fetchone()[0]}")
cur.execute("SELECT COUNT(*) FROM oltp_sensor.sensor_rendimiento")
print(f"Registros sensor rendimiento: {cur.fetchone()[0]}")

cur.close()
conn.close()
print("\nTodo completado correctamente")