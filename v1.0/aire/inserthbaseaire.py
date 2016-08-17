### Parte de leer los datos de hdfs ###
import pandas as pd
import numpy as np
import pydoop.hdfs as hdfs
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
        [ SO2:0-23,CO:0-23,NO2:0-23,PM2.5:0-23,
        PM10:0-23,NOx:0-23,O3:0-23,BEN:0-23 ]
    """
    batch.put(row[0],{"SO2:0":row[1], "SO2:1":row[2], "SO2:2":row[3], "SO2:3":row[4], "SO2:4":row[5], "SO2:5":row[6], "SO2:6":row[7], "SO2:7":row[8], "SO2:8":row[9], "SO2:9":row[10],
    "SO2:10":row[11], "SO2:11":row[12], "SO2:12":row[13], "SO2:13":row[14], "SO2:14":row[15], "SO2:15":row[16], "SO2:16":row[17], "SO2:17":row[18], "SO2:18":row[19],
    "SO2:19":row[20], "SO2:20":row[21], "SO2:21":row[22], "SO2:22":row[23], "SO2:23":row[24],
    "CO:0":row[25], "CO:1":row[26], "CO:2":row[27], "CO:3":row[28], "CO:4":row[29], "CO:5":row[30], "CO:6":row[31], "CO:7":row[32], "CO:8":row[33], "CO:9":row[34],
    "CO:10":row[35], "CO:11":row[36], "CO:12":row[37], "CO:13":row[38], "CO:14":row[39], "CO:15":row[40], "CO:16":row[41], "CO:17":row[42], "CO:18":row[43],
    "CO:19":row[44], "CO:20":row[45], "CO:21":row[46], "CO:22":row[47], "CO:23":row[48],
    "NO2:0":row[49], "NO2:1":row[50], "NO2:2":row[51], "NO2:3":row[52], "NO2:4":row[53], "NO2:5":row[54], "NO2:6":row[55], "NO2:7":row[56], "NO2:8":row[57], "NO2:9":row[58],
    "NO2:10":row[59], "NO2:11":row[60], "NO2:12":row[61], "NO2:13":row[62], "NO2:14":row[63], "NO2:15":row[64], "NO2:16":row[65], "NO2:17":row[66], "NO2:18":row[67],
    "NO2:19":row[68], "NO2:20":row[69], "NO2:21":row[70], "NO2:22":row[71], "NO2:23":row[72],
    "PM2.5:0":row[73], "PM2.5:1":row[74], "PM2.5:2":row[75], "PM2.5:3":row[76], "PM2.5:4":row[77], "PM2.5:5":row[78], "PM2.5:6":row[79], "PM2.5:7":row[80], "PM2.5:8":row[81],
    "PM2.5:9":row[82], "PM2.5:10":row[83], "PM2.5:11":row[84], "PM2.5:12":row[85], "PM2.5:13":row[86], "PM2.5:14":row[87], "PM2.5:15":row[88], "PM2.5:16":row[89],
    "PM2.5:17":row[90], "PM2.5:18":row[91], "PM2.5:19":row[92], "PM2.5:20":row[93], "PM2.5:21":row[94], "PM2.5:22":row[95], "PM2.5:23":row[96],
    "PM10:0":row[97], "PM10:1":row[98], "PM10:2":row[99], "PM10:3":row[100], "PM10:4":row[101], "PM10:5":row[102], "PM10:6":row[103], "PM10:7":row[104], "PM10:8":row[105],
    "PM10:9":row[106], "PM10:10":row[107], "PM10:11":row[108], "PM10:12":row[109], "PM10:13":row[110], "PM10:14":row[111], "PM10:15":row[112], "PM10:16":row[113],
    "PM10:17":row[114], "PM10:18":row[115], "PM10:19":row[116], "PM10:20":row[117], "PM10:21":row[118], "PM10:22":row[119], "PM10:23":row[120],
    "NOx:0":row[121], "NOx:1":row[122], "NOx:2":row[123], "NOx:3":row[124], "NOx:4":row[125], "NOx:5":row[126], "NOx:6":row[127], "NOx:7":row[128], "NOx:8":row[129],
    "NOx:9":row[130], "NOx:10":row[131], "NOx:11":row[132], "NOx:12":row[133], "NOx:13":row[134], "NOx:14":row[135], "NOx:15":row[136], "NOx:16":row[137], "NOx:17":row[138],
    "NOx:18":row[139], "NOx:19":row[140], "NOx:20":row[141], "NOx:21":row[142], "NOx:22":row[143], "NOx:23":row[144],
    "O3:0":row[145], "O3:1":row[146], "O3:2":row[147], "O3:3":row[148], "O3:4":row[149], "O3:5":row[150], "O3:6":row[151], "O3:7":row[152], "O3:8":row[153], "O3:9":row[154],
    "O3:10":row[155], "O3:11":row[156], "O3:12":row[157], "O3:13":row[158], "O3:14":row[159], "O3:15":row[160], "O3:16":row[161], "O3:17":row[162], "O3:18":row[163], "O3:19":row[164],
    "O3:20":row[165], "O3:21":row[166], "O3:22":row[167], "O3:23":row[168],
    "BEN:0":row[169], "BEN:1":row[170], "BEN:2":row[171], "BEN:3":row[172], "BEN:4":row[173], "BEN:5":row[174], "BEN:6":row[175], "BEN:7":row[176], "BEN:8":row[177], "BEN:9":row[178],
    "BEN:10":row[179], "BEN:11":row[180], "BEN:12":row[181], "BEN:13":row[182], "BEN:14":row[183], "BEN:15":row[184], "BEN:16":row[185], "BEN:17":row[186], "BEN:18":row[187], "BEN:19":row[188],
    "BEN:20":row[189], "BEN:21":row[190], "BEN:22":row[191], "BEN:23":row[192]})

#para renombrar las columnas al leer el archivo
columnas = []
for i in range(57):
    aux = 'c'+str(i)
    columnas.append(aux)
## para coger todos los archivos de la carpeta
conn_hdfs = hdfs.fs.hdfs()
auxiliar =  conn_hdfs.list_directory("/user/datosaire/sin_tratar/")

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
	with hdfs.open("/user/datosaire/sin_tratar/"+k) as archivo:
		datos = pd.read_csv(archivo,names=columnas)
        #creamos en la columna cero el valor de la estacion
        datos['c0'] = datos['c0']*1000000+datos['c1']*1000+datos['c2']
        datos.rename(columns = {'c0':'estacion','c3':'magnitud','c6':'anio','c7':'mes','c8':'dia'}, inplace = True)
        #quitamos las columnas que utilizamos para formar la estacion y las otras dos que contienen datos que no nos sirven
        datos = datos.drop(datos.columns[[1,2,4,5]],axis=1)
        #filtramos en una dataframe auxiliar solo las estaciones que queremos y las magnitudes que queremos utilizar
        auxiliar = datos.loc[datos['estacion'].isin([28079024,28079018,28079047,28079049,28079008,28079036,28079011,28079039])]
        auxiliar = auxiliar.loc[auxiliar['magnitud'].isin([1,6,8,9,10,12,14,30])]
        #para crear la fecha
        if auxiliar.iloc[1,3] in [1,2,3,4,5,6,7,8,9]:
            if auxiliar.iloc[1,4] in [1,2,3,4,5,6,7,8,9]:
                fecha = str(auxiliar.iloc[1,2])+'0'+str(auxiliar.iloc[1,3])+'0'+str(auxiliar.iloc[1,4])
            else:
                fecha = str(auxiliar.iloc[1,2])+'0'+str(auxiliar.iloc[1,3])+str(auxiliar.iloc[1,4])
        else:
            fecha = str(auxiliar.iloc[1,2])+str(auxiliar.iloc[1,3])+str(auxiliar.iloc[1,4])
        #quitamos las columnas de estacion ya que ya nos nos sirve, la de la fecha que tampoco ya nos sirve y la de las letras de V o N del valor
        auxiliar = auxiliar.drop(auxiliar.columns[[0,2,3,4,6,8,10,12,14,16,18,20,22,24,26,28,30,32,34,36,38,40,42,44,46,48,50,52]],axis=1)
        #array donde se almacena las medias para subir a hbase
        fila = [fecha]
        #bucle que lee todo el dataframe auxiliar para hacer la media de cada magnitud para todas las estaciones y asi almacenar un solo dato en el array final estacion
        magnitudes = [1,6,8,9,10,12,14,30]
        for i in magnitudes:
            for j in range(1,len(auxiliar.columns)):
                auxiliar_media = auxiliar.loc[auxiliar['magnitud']== i]
                fila.append(str(auxiliar_media.iloc[:,j].mean()))
        ### Parte de meter los datos en hbase con el conector ###
        batch_size = 1000
        host = 'localhost'
        namespace = "calidadaire"
        row_count = 0
        start_time = time.time()
        table_name = "medicion_aire"
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
        hdfs.move("/user/datosaire/sin_tratar/"+k, "/user/datosaire/tratado/")
