import pandas as pd
import pymysql
import re

# Leer el dataset
dataset_path = 'datasetcarreras.csv'
df = pd.read_csv(dataset_path)

# Conexión a la base de datos
conn = pymysql.connect(
    host='localhost',  # Reemplaza con tu host
    user='root',  # Reemplaza con tu usuario
    password='',  # Reemplaza con tu contraseña
    database='carreras'  # Reemplaza con el nombre de tu base de datos
)
cursor = conn.cursor()


# Función para limpiar el nombre del caballo
def limpiar_nombre(nombre):
    # Elimina cualquier texto entre paréntesis al final del nombre
    return re.sub(r'\s*\(.*?\)\s*$', '', nombre)


# Crear columnas adicionales en el DataFrame
df['Familia1_1'] = None
df['Familia1_2'] = None
df['Familia2_1'] = None
df['Familia2_2'] = None
df['Familia2_3'] = None
df['Familia2_4'] = None

# Iterar sobre el DataFrame y buscar los registros en la base de datos
for index, row in df.iterrows():
    nombre_caballo = limpiar_nombre(row['nombre'])

    # Buscar el registro del caballo en la base de datos
    cursor.execute("""
        SELECT Familia1_1, Familia1_2, Familia2_1, Familia2_2, Familia2_3, Familia2_4
        FROM caballos
        WHERE Nombre = %s
    """, (nombre_caballo,))

    result = cursor.fetchone()

    # Si se encuentra el registro, actualizar el DataFrame
    if result:
        df.at[index, 'Familia1_1'] = result[0]
        df.at[index, 'Familia1_2'] = result[1]
        df.at[index, 'Familia2_1'] = result[2]
        df.at[index, 'Familia2_2'] = result[3]
        df.at[index, 'Familia2_3'] = result[4]
        df.at[index, 'Familia2_4'] = result[5]

# Guardar el DataFrame actualizado
output_path = 'datasetcarreras_actualizado.csv'
df.to_csv(output_path, index=False)

# Cerrar la conexión a la base de datos
conn.close()

print(f'Dataset actualizado guardado en: {output_path}')
