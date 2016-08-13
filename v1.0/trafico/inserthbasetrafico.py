### Parte de leer los datos de hdfs ###
import pandas as pd
import numpy as np
import pydoop.hdfs as hdfs
from lxml import objectify
import csv
import happybase
import time

#funcion para conectar a hbase con las parametros de namespace y la table_name
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

#funcion para meter los datos
def insert_row(batch, row):
    """ Insert a row into HBase.
    Write the row to the batch. When the batch size is reached, rows will be
    sent to the database.
    Rows have the following schema:
        [ total_vehiculos_tunel:0-23,total_vehiculos_calle30:0-23,
        velocidad_media_superficie:0-23,velocidad_media_tunel:0-23 ]
    """
    batch.put(row[0],{"total_vehiculos_tunel:0":row[1],"total_vehiculos_calle30:0":row[2],"velocidad_media_superficie:0":row[3],"velocidad_media_tunel:0":row[4]})

## para coger todos los archivos de la carpeta
conn_hdfs = hdfs.fs.hdfs()
auxiliar =  conn_hdfs.list_directory("/user/datostrafico/sin_tratar/")

archivos = []
for i in range(len(auxiliar)):
	#cogemos de todo el array uno individual y luego el campo especifico
	a = auxiliar[i]['name']
	#troceamos todo la cadena que es la ruta entera
	b = a.split('/')
	#como queremos coger el ultimo hacemos el len y luego le restaras uno
	c = len(b)
	d = b[c-1]
	#metemos todos en el array que creamos
	archivos.append(d)

for k in archivos:
    with hdfs.open("/user/datostrafico/sin_tratar/"+k) as archivo:
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
    #esta variable es para coger el nombre del archivo
    troceo = k.split('_')
    fila = [troceo[0],str(totalvehiculostunel),str(totalvehiculoscalle30),str(velocidadmediasuperficie),str(velocidadmediatunel)]
    print fila
    ### Parte de meter los datos en hbase con el conector ###
    # Parametros de configuracion
    batch_size = 1000
    #host = "192.168.1.108"
    host = 'localhost'
    namespace = "calidadaire"
    row_count = 0
    start_time = time.time()
    table_name = "medicion_trafico"
    # After everything has been defined, run the script.
    conn, batch = connect_to_hbase()
    print "Connect to HBase. table name: %s, batch size: %i" % (table_name, batch_size)
    try:
        insert_row(batch, fila)
        batch.send()
    finally:
        # No matter what happens, close the file handle.
        conn.close()
    #finalmente movemos el archivo a a ya tratado
    hdfs.move("/user/datostrafico/sin_tratar/"+k, "/user/datostrafico/tratado/")
