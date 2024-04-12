import csv
import sys
import MySQLdb

def leer_csv(ruta_archivo):
    datos = []
    with open(ruta_archivo, mode='r', newline='', encoding='utf-8') as archivo_csv:
        lector_csv = csv.DictReader(archivo_csv)
        for fila in lector_csv:
            datos.append(fila)
    return datos

try:
    #conexion a MySQL
    db = MySQLdb.connect('localhost','root','','database')
    print('Conexion correcta')
except MySQLdb.Error as e:
    print('no se conecto a la base de datos: ', e)
    sys.exit(1)




def crear_tabla(db):
    cursor = db.cursor()
    # Verificar si la tabla ya existe
    cursor.execute(" DROP TABLE IF EXISTS provincias ")

    # Crear la tabla
    cursor.execute('''CREATE TABLE IF NOT EXISTS provincias (
                      provincia VARCHAR(100),
                      id INT,
                      localidad VARCHAR(100),
                      cp VARCHAR(100),
                      id_prov_mstr VARCHAR(100)
                      )''')
    db.commit()



def insertar_datos(db, datos):
    cursor = db.cursor()
    for fila in datos:
        cursor.execute('''INSERT INTO provincias (provincia,id,localidad, cp, id_prov_mstr ) VALUES (%s ,%s, %s, %s, %s)''', (fila['provincia'],fila['id'],fila['localidad'],fila['cp'],fila['id_prov_mstr']))
    db.commit()

def agrupar_por_provincia(db):
    cursor = db.cursor()
    cursor.execute('''SELECT provincia, GROUP_CONCAT(localidad, ', ') AS localidades FROM provincias GROUP BY provincia''')
    return cursor.fetchall()

def exportar_a_csv(datos_por_provincia):
    for provincia, localidades in datos_por_provincia:
        with open(f'{provincia}.csv', 'w', newline='') as archivo_csv:
            escritor_csv = csv.writer(archivo_csv)
            escritor_csv.writerow(['Localidades'])
            for localidad in localidades.split(', '):
                escritor_csv.writerow([localidad])

# Uso de las funciones
datos = leer_csv('localidades.csv')
crear_tabla(db)
insertar_datos(db, datos)
datos_por_provincia = agrupar_por_provincia(db)
exportar_a_csv(datos_por_provincia)
db.close()