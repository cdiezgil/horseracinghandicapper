# Este programa hace webscrapping de la web www.todoturf.com
# Primero descarga todos los caballos de su base de datos, parsea los datos y lo almacena en una tabla de mysql: caballo
# Después descarga todas las carreras desde 1991, parsea los datos y los almacena en mysql, en dos tablas: carrera y carrera_caballo

import os
import requests
from bs4 import BeautifulSoup
from bs4.element import Tag
import csv
from urllib.parse import quote

directorio = "/Users/carlosdiez/Data/"


def find_div_cuerpo(element):
    if element.name == "div" and "id" in element.attrs and element["id"] == "cuerpo":
        return element
    for child in element.children:
        if isinstance(child, Tag):
            result = find_div_cuerpo(child)
            if result is not None:
                return result
    return None


import pymysql

from datetime import datetime


def extraer_anio(fecha_str):
    # Parsea el string de fecha del formato DD-MM-YYYY
    fecha = datetime.strptime(fecha_str, '%d/%m/%Y')
    # Devuelve el año como un entero
    return fecha.year


def solo_numeros(cadena):
    if cadena == 0:
        return ("0")
    resultado = ''.join([caracter for caracter in cadena if caracter.isdigit() or caracter == '.'])
    return resultado


def convertir_fecha(fecha_str):
    # Convierte un string en formato DD/MM/YYYY a YYYY-MM-DD
    return datetime.strptime(fecha_str, '%d/%m/%Y').strftime('%Y-%m-%d')


def convertir_punto_decimal(numero):
    if numero == "":
        return 0
    else:
        return numero.replace(',', '.')


def convertir_tiempo(tiempo_str):
    # Asume que el tiempo está en el formato mm:ss:dd (dd son décimas de segundo)
    if tiempo_str == "":
        return f"00:{0}:{0}.{0}"
    tiempo_str = tiempo_str.replace('.', ':')
    tiempo_str = tiempo_str.replace(';', ':')
    partes = tiempo_str.split(':')

    minutos = partes[0]
    segundos = partes[1]
    if len(partes)>2:
        decimas = partes[2]
    else:
        decimas = 0

    # Convertir décimas de segundo a milisegundos
    milisegundos = int(decimas) * 100

    # Construir el nuevo formato de tiempo
    tiempo_mysql = f"00:{minutos}:{segundos}.{milisegundos}"
    return tiempo_mysql


def insertar_carrera_y_participantes(distancia, dotacion_ganador, estado, fecha, hipodromo, nombre_carrera,
                                     participantes, pista, tiempo, codigo_carrera):
    # Conexión a la base de datos
    connection = pymysql.connect(host='localhost',
                                 user='root',
                                 password='',
                                 database='carreras',
                                 cursorclass=pymysql.cursors.DictCursor)

    codigo_carrera_anio = str(extraer_anio(fecha)) + "-" + codigo_carrera

    try:
        with connection.cursor() as cursor:
            # Insertar datos en la tabla Carreras
            sql_carrera = "INSERT INTO Carreras (codigo_carrera, distancia, dotacion_ganador, estado, fecha, hipodromo, nombre_carrera, pista, tiempo) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
            cursor.execute(sql_carrera, (codigo_carrera_anio, distancia, dotacion_ganador, estado,
                                         convertir_fecha(fecha), hipodromo, nombre_carrera, pista,
                                         convertir_tiempo(tiempo)))

            # Insertar datos en la tabla Participantes
            sql_participante = "INSERT INTO Participantes (codigo_carrera, puesto, nombre, peso, jockey, distancia_caballo, valor, premio) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
            for caballo in participantes:
                cursor.execute(sql_participante, (
                    codigo_carrera_anio, caballo[0], caballo[1],
                    solo_numeros(convertir_punto_decimal(caballo[2])),
                    caballo[3], convertir_punto_decimal(caballo[4]), convertir_punto_decimal(caballo[5]),
                    caballo[6]))

        # Comprometer la transacción
        connection.commit()

    except pymysql.MySQLError as e:
        print("Error de MySQL:", e)
    finally:
        connection.close()


def procesarHTML(enlaces_carreras, host):
    # Itera sobre los enlaces y realiza scraping de los datos de cada carrera
    for enlace in enlaces_carreras:
        # Obtén la URL del enlace
        if host == "":
            url_carrera = enlace
        else:
            url_carrera = host + enlace["href"]
            url_carrera = url_carrera.replace(" ", "%20")
        if (url_carrera.find("videocarrera.php") > 0):
            continue
        print("Obteniendo datos de la carrera en {}".format(url_carrera))

        # Realiza una solicitud HTTP a la URL de la carrera y obtén su contenido HTML
        response = requests.get(url_carrera)
        contenido_carrera = response.content
        if response.status_code != 200:
            print("No se pudo obtener el contenido de la carrera en {}".format(url_carrera))
            continue
        # Parsea el contenido HTML de la carrera utilizando BeautifulSoup
        soup_carrera = BeautifulSoup(contenido_carrera, "html.parser")

        # Encontrar el elemento div con id "cuerpo"
        div_cuerpo = find_div_cuerpo(soup_carrera)

        # Verificar si se encontró el elemento
        if div_cuerpo is not None:

            # Extraer el contenido dentro del div
            html = div_cuerpo.prettify()
            # Parsear el contenido HTML
            soupDiv = BeautifulSoup(html, "html.parser")

            # Busco todas las negritas en la pagina
            negritas = soupDiv.find_all("b")
            if len(negritas) < 8:
                print("No se encontraron los datos de la carrera")
                continue
            # Fecha
            fecha = negritas[0].text.strip()
            fecha = fecha.replace("(", "")
            fecha = fecha.replace(")", "")
            # Nombre del premio
            nombre_carrera = negritas[1].text.strip()
            codigo_carrera = nombre_carrera.split(" ")[0]
            nombre_carrera = nombre_carrera.replace(codigo_carrera, "")
            nombre_carrera = nombre_carrera.replace("/", "-")
            codigo_carrera = codigo_carrera.replace("(", "")
            codigo_carrera = codigo_carrera.replace(")", "")

            # Hipódromo
            hipodromo = negritas[2].text.strip()

            # Pista
            pista = negritas[3].text.strip()

            # Estado
            estado = negritas[4].text.strip()

            # Tiempo
            tiempo = negritas[5].text.strip()

            # Distancia
            distancia = negritas[6].text.strip()
            distancia = distancia.replace(" m.", "")
            distancia = distancia.replace(".", "")

            # Dotación Ganador
            dotacion_ganador = negritas[7].text.strip()
            dotacion_ganador = dotacion_ganador.replace(" €", "")
            dotacion_ganador = dotacion_ganador.replace(".", "")

            participantes = []
            # Extraer los detalles de los caballos participantes
            caballosTr = soupDiv.find_all("tr")

            for caballo in caballosTr[1:]:  # Omitir la primera fila que contiene los encabezados de las columnas
                celdas = caballo.find_all("td")
                puesto = celdas[0].text.strip()
                nombre = celdas[1].text.strip()
                peso = celdas[2].text.strip()
                jockey = celdas[3].text.strip()
                distancia_caballo = celdas[4].text.strip()
                valor = celdas[5].text.strip()
                valor = valor.replace(" ", "")
                premio_caballo = celdas[6].text.strip()
                premio_caballo = premio_caballo.replace(" €", "")
                premio_caballo = premio_caballo.replace(".", "")
                participantes.append([puesto, nombre, peso, jockey, distancia_caballo, valor, premio_caballo])

            insertar_carrera_y_participantes(distancia, dotacion_ganador, estado, fecha, hipodromo, nombre_carrera,
                                             participantes, pista,
                                             tiempo, codigo_carrera)


    else:
        print("No se encontró el elemento <div> con id='cuerpo'")


# Este metodo, recorre todos los ficheros de un directorio buscando html, y por cada uno de ellos saca los hipervinculos
# que es donde estan las carreras. Por cada carrera, la procesa.
def procesarDatos():
    directorio_carreras = "htmlCarreras"
    archivos_html = os.listdir(directorio_carreras)
    archivos_html = [archivo for archivo in archivos_html if archivo.endswith(".html")]
    print("Encontrados {} archivos HTML".format(len(archivos_html)))
    for archivo in archivos_html:
        # Obtén la ruta completa del archivo HTML
        ruta_archivo = os.path.join(directorio_carreras, archivo)
        # Abre el archivo y lee su contenido HTML
        with open(ruta_archivo, "r") as f:
            contenido_html = f.read()

        # Parsea el contenido HTML utilizando BeautifulSoup
        soup = BeautifulSoup(contenido_html, "html.parser")

        # Encuentra los enlaces a las carreras en la tabla o sección correspondiente
        enlaces = soup.find_all("a")
        host_carreras = "https://www.todoturf.net/Html3/"
        print("Encontradas {} carreras en {}".format(len(enlaces), archivo))

        procesarHTML(enlaces, host_carreras)


def test_carrera(url):
    procesarHTML([url], "")


procesarDatos()
