### Parte de leer los datos de hdfs ###
import pandas as pd
import numpy as np
import pydoop.hdfs as hd
from lxml import objectify

with hd.open("/user/datostiempo/20160525_1341.xml") as archivo:
    parsed = objectify.parse(archivo)

root = parsed.getroot()
prob_precipitacion = []
estado_cielo =[]
viento = []
temperatura = []
tempmax = []
tempmin = []
iteraccion = 0
errores = []
print "root : ", root
for row in root.prediccion.dia:
    for row_precipitacion in row.prob_precipitacion:
        aux_precipitacion = []
        if (row_precipitacion != ''):
            aux_precipitacion.append(row_precipitacion)
        else:
            errores.append(1)
    prob_precipitacion.append(str(sum(aux_precipitacion) / float(len(aux_precipitacion))))
    for row_cielo in row.estado_cielo:
        aux_cielo = []
        if (row_cielo != ''):
            try:
                int(row_cielo)
                aux_cielo.append(row_cielo)
            except ValueError:
                errores.append(1)
        else:
            errores.append(1)
    estado_cielo.append(str(sum(aux_cielo) / len(aux_cielo)))
    for row_viento in row.viento:
        aux_viento = []
        if (row_viento.velocidad != ''):
            aux_viento.append(row_viento.velocidad)
        else:
            errores.append(1)
    viento.append(str(sum(aux_viento) / float(len(aux_viento))))
    for row_temp in row.temperatura:
        aux_temp = []
        tempmax.append(str(row_temp.maxima))
        tempmin.append(str(row_temp.minima))
        if (iteraccion < 2):
            for datos in row_temp.dato:
                aux_temp.append(datos)
            temperatura.append(str(sum(aux_temp) / float(len(aux_temp))))
    iteraccion = iteraccion + 1

fila = prob_precipitacion+estado_cielo+viento+tempmax+tempmin
### Parte de meter los datos en hbase con el conector ###

import csv
import happybase
import time

batch_size = 1000
#host = "192.168.1.108"
host = 'localhost'
namespace = "calidadaire"
row_count = 0
start_time = time.time()
table_name = "medicion_tiempo"


def connect_to_hbase():
    """ Connect to HBase server.
    This will use the host, namespace, table name, and batch size as defined in
    the global variables above.
    """
    conn = happybase.Connection(host = host,
        table_prefix = namespace,
        table_prefix_separator = ":")
    conn.open()
    table = conn.table(table_name)
    batch = table.batch(batch_size = batch_size)

    return conn, batch

def insert_row(batch, row):
    """ Insert a row into HBase.
    Write the row to the batch. When the batch size is reached, rows will be
    sent to the database.
    Rows have the following schema:
        [ precipitacion:1-7,estado_cielo:1-7,viento:1-7,
        temperatura_maxima:1-7,temperatura_minima:1-7 ]
    """
    batch.put("20160621",{"precipitaciones:1":row[0],"precipitaciones:2":row[1],"precipitaciones:3":row[2],"precipitaciones:4":row[3],"precipitaciones:5":row[4],"precipitaciones:6":row[5],"precipitaciones:7":row[6],
    "estado_cielo:1":row[7],"estado_cielo:2":row[8],"estado_cielo:3":row[9],"estado_cielo:4":row[10],"estado_cielo:5":row[11],"estado_cielo:6":row[12],"estado_cielo:7":row[13],
    "viento:1":row[14],"viento:2":row[15],"viento:3":row[16],"viento:4":row[17],"viento:5":row[18],"viento:6":row[19],"viento:7":row[20],
    "temperatura_maxima:1":row[21],"temperatura_maxima:2":row[22],"temperatura_maxima:3":row[23],"temperatura_maxima:4":row[24],"temperatura_maxima:5":row[25],"temperatura_maxima:6":row[26],"temperatura_maxima:7":row[27],
    "temperatura_minima:1":row[28],"temperatura_minima:2":row[29],"temperatura_minima:3":row[30],"temperatura_minima:4":row[31],"temperatura_minima:5":row[32],"temperatura_minima:6":row[33],"temperatura_minima:7":row[34]})



# After everything has been defined, run the script.
conn, batch = connect_to_hbase()
#	print "Connect to HBase. table name: %s, batch size: %i" % (table_name, batch_size)

try:

    insert_row(batch, fila)

    batch.send()
finally:
    # No matter what happens, close the file handle.
    conn.close()
