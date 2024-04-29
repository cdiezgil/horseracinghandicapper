import os
import pandas as pd
import pymysql
from sqlalchemy import create_engine

# Conexión a MySQL
usuario = 'root'
contrasena = ''
host = 'localhost'
database = 'carreras'
conexion_str = f'mysql+pymysql://{usuario}:{contrasena}@{host}/{database}'
engine = create_engine(conexion_str)

# Directorio donde se encuentran tus archivos CSV
directorio = '/Users/carlosdiez/Projects/carreras/caballos'

mapeo_nombres = {
    'Año nacimiento': 'AnoNacimiento',
    'Familia 1 1': 'Familia1_1',
    'Familia 1 2': 'Familia1_2',
    'Familia 2 1': 'Familia2_1',
    'Familia 2 2': 'Familia2_2',
    'Familia 2 3': 'Familia2_3',
    'Familia 2 4': 'Familia2_4',
    'Familia 3 1': 'Familia3_1',
    'Familia 3 2': 'Familia3_2',
    'Familia 3 3': 'Familia3_3',
    'Familia 3 4': 'Familia3_4',
    'Familia 3 5': 'Familia3_5',
    'Familia 3 6': 'Familia3_6',
    'Familia 3 7': 'Familia3_7',
    'Familia 3 8': 'Familia3_8',
    'Familia 4 1': 'Familia4_1',
    'Familia 4 2': 'Familia4_2',
    'Familia 4 3': 'Familia4_3',
    'Familia 4 4': 'Familia4_4',
    'Familia 4 5': 'Familia4_5',
    'Familia 4 6': 'Familia4_6',
    'Familia 4 7': 'Familia4_7',
    'Familia 4 8': 'Familia4_8',
    'Familia 4 9': 'Familia4_9',
    'Familia 4 10': 'Familia4_10',
    'Familia 4 11': 'Familia4_11',
    'Familia 4 12': 'Familia4_12',
    'Familia 4 13': 'Familia4_13',
    'Familia 4 14': 'Familia4_14',
    'Familia 4 15': 'Familia4_15',
    'Familia 4 16': 'Familia4_16',
    'Familia 5 1': 'Familia5_1',
    'Familia 5 2': 'Familia5_2',
    'Familia 5 3': 'Familia5_3',
    'Familia 5 4': 'Familia5_4',
    'Familia 5 5': 'Familia5_5',
    'Familia 5 6': 'Familia5_6',
    'Familia 5 7': 'Familia5_7',
    'Familia 5 8': 'Familia5_8',
    'Familia 5 9': 'Familia5_9',
    'Familia 5 10': 'Familia5_10',
    'Familia 5 11': 'Familia5_11',
    'Familia 5 12': 'Familia5_12',
    'Familia 5 13': 'Familia5_13',
    'Familia 5 14': 'Familia5_14',
    'Familia 5 15': 'Familia5_15',
    'Familia 5 16': 'Familia5_16',
    'Familia 5 17': 'Familia5_17',
    'Familia 5 18': 'Familia5_18',
    'Familia 5 19': 'Familia5_19',
    'Familia 5 20': 'Familia5_20',
    'Familia 5 21': 'Familia5_21',
    'Familia 5 22': 'Familia5_22',
    'Familia 5 23': 'Familia5_23',
    'Familia 5 24': 'Familia5_24',
    'Familia 5 25': 'Familia5_25',
    'Familia 5 26': 'Familia5_26',
    'Familia 5 27': 'Familia5_27',
    'Familia 5 28': 'Familia5_28',
    'Familia 5 29': 'Familia5_29',
    'Familia 5 30': 'Familia5_30',
    'Familia 5 31': 'Familia5_31',
    'Familia 5 32': 'Familia5_32',
}
# Diccionario para mapear nombres de columnas de CSV a nombres de columnas de MySQL

# Recorrer cada archivo en el directorio
for archivo in os.listdir(directorio):
    if archivo.endswith('.csv'):
        ruta_completa = os.path.join(directorio, archivo)
        # Leer el archivo CSV
        data = pd.read_csv(ruta_completa)

        # Renombrar las columnas según el mapeo
        data.rename(columns=mapeo_nombres, inplace=True)

        # Insertar los datos en la base de datos
        data.to_sql('Caballos', con=engine, if_exists='append', index=False)
        print(f'Datos de {archivo} insertados con éxito.')

print('Todos los archivos han sido procesados.')
