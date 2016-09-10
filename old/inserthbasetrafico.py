### Parte de leer los datos de hdfs ###
import pandas as pd
import numpy as np
import pydoop.hdfs as hd
from lxml import objectify

with hd.open("/user/datostrafico/20160526_1711.xml") as archivo:
    parsed = objectify.parse(archivo)

DatosTrafico = parsed.getroot()
totalvehiculostunel = 0;
totalvehiculoscalle30 = 0;
velocidadmediatunel = 0;
velocidadmediasuperficie = 0;
iteraccion = 0
for row in DatosTrafico.DatoGlobal:
    if (row.Nombre == 'totalVehiculosTunel'):
        totalvehiculostunel = row.VALOR
    if (row.Nombre == 'totalVehiculosCalle30'):
        totalvehiculoscalle30 = row.VALOR
    if (row.Nombre == 'velocidadMediaTunel'):
        velocidadmediatunel = row.VALOR
    if (row.Nombre == 'velicidadMediaSuperfice'):
        velocidadmediasuperficie = row.VALOR

    iteraccion = iteraccion + 1

fila = [str(totalvehiculostunel),str(totalvehiculoscalle30),str(velocidadmediasuperficie),str(velocidadmediatunel)]
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
table_name = "medicion_trafico"


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
        [ total_vehiculos_tunel:0-23,total_vehiculos_calle30:0-23,
        velocidad_media_superficie:0-23,velocidad_media_tunel:0-23 ]
    """
    batch.put("20160621",{"total_vehiculos_tunel:0":row[0],"total_vehiculos_calle30:0":row[1],"velocidad_media_superficie:0":row[2],"velocidad_media_tunel:0":row[3]})

# After everything has been defined, run the script.
conn, batch = connect_to_hbase()
#	print "Connect to HBase. table name: %s, batch size: %i" % (table_name, batch_size)

try:

    insert_row(batch, fila)

    batch.send()
finally:
    # No matter what happens, close the file handle.
    conn.close()
