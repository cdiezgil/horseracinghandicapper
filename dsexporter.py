import pymysql
import pandas as pd

# Conexión a la base de datos
connection = pymysql.connect(host='localhost',
                             user='root',
                             password='',
                             database='carreras',
                             cursorclass=pymysql.cursors.DictCursor)

try:
    with connection.cursor() as cursor:
        # Query para extraer los datos necesarios
        query = """
        SELECT c.fecha, c.nombre_carrera, c.hipodromo, c.distancia, c.pista, c.estado, c.dotacion_ganador,
               cc.puesto, cc.nombre, cc.peso, cc.jockey, cc.distancia_caballo, cc.valor, cc.premio
        FROM Carreras c
        JOIN Participantes cc ON c.codigo_carrera = cc.codigo_carrera
        """
        cursor.execute(query)
        # Obtener los datos en un DataFrame
        data = pd.DataFrame(cursor.fetchall())

        # Convertir la columna 'fecha' a datetime
        data['fecha'] = pd.to_datetime(data['fecha'])

        # Rellenar valores nulos en la columna 'estado' con "Buena"
        data['estado'] = data['estado'].fillna("Buena")

        # Aquí puedes añadir más procesamiento según sea necesario

finally:
    connection.close()

# Guardar el DataFrame en un archivo CSV para su uso en modelado
data.to_csv('datasetcarreras.csv', index=False)