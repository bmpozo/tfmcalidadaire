### Parte de leer los datos de hdfs ###
import pandas as pd
import numpy as np
import pydoop.hdfs as hd

columnas = []
for i in range(57):
    aux = 'c'+str(i)
    columnas.append(aux)
with hd.open("/user/datosaire/20160526_1708.txt") as archivo:
    datos = pd.read_csv(archivo,names=columnas)

datos['c0'] = datos['c0']*1000000+datos['c1']*1000+datos['c2']
datos.rename(columns = {'c0':'estacion','c3':'magnitud','c6':'anio','c7':'mes','c8':'dia'}, inplace = True)

datos.drop(datos.columns[[1,2,4,5]],axis=1)

auxiliar = datos[datos['estacion']==28079024]
auxiliar = auxiliar.loc[auxiliar['magnitud'].isin([1,6,8,9,10,12,14,30])]

fecha = str(auxiliar.iloc[1,2])+str(auxiliar.iloc[1,3])+str(auxiliar.iloc[1,4])
#estacion = [auxiliar.iloc[0,0],fecha]
estacion = [fecha]
for i in range(len(auxiliar)):
    for j in range(5,len(auxiliar.columns)):
        if j%2:
            estacion.append(str(auxiliar.iloc[i,j]))

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
table_name = "medicion_aire"


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
        [ SO2:0-23,NOx:0-23,PM10:0-23,
        PM2.5:0-23,O3:0-23,C6H6:0-23,CO:0-23 ]
    """
    batch.put(row[0],{"SO2:0":row[1],"SO2:1":row[2],"SO2:2":row[3],"SO2:3":row[4],"SO2:4":row[5],"SO2:5":row[6],"SO2:6":row[7],"SO2:7":row[8],"SO2:8":row[9],"SO2:9":row[10],
    "SO2:10":row[11],"SO2:11":row[12],"SO2:12":row[13],"SO2:13":row[14],"SO2:14":row[15],"SO2:15":row[16],"SO2:16":row[17],"SO2:17":row[18],"SO2:18":row[19],"SO2:19":row[20],
    "SO2:20":row[21],"SO2:21":row[22],"SO2:22":row[23],"SO2:23":row[24],
    "NOx:0":row[25],"NOx:1":row[26],"NOx:2":row[27],"NOx:3":row[28],"NOx:4":row[29],"NOx:5":row[30],"NOx:6":row[31],"NOx:7":row[32],"NOx:8":row[33],"NOx:9":row[34],
    "NOx:10":row[35],"NOx:11":row[36],"NOx:12":row[37],"NOx:13":row[38],"NOx:14":row[39],"NOx:15":row[40],"NOx:16":row[41],"NOx:17":row[42],"NOx:18":row[43],"NOx:19":row[44],
    "NOx:20":row[45],"NOx:21":row[46],"NOx:22":row[47],"NOx:23":row[48],
    "PM10:0":row[49],"PM10:1":row[50],"PM10:2":row[51],"PM10:3":row[52],"PM10:4":row[53],"PM10:5":row[54],"PM10:6":row[55],"PM10:7":row[56],"PM10:8":row[57],"PM10:9":row[58],
    "PM10:10":row[59],"PM10:11":row[60],"PM10:12":row[61],"PM10:13":row[62],"PM10:14":row[63],"PM10:15":row[64],"PM10:16":row[65],"PM10:17":row[66],"PM10:18":row[67],"PM10:19":row[68],
    "PM10:20":row[69],"PM10:21":row[70],"PM10:22":row[71],"PM10:23":row[72],
    "PM2.5:0":row[73],"PM2.5:1":row[74],"PM2.5:2":row[75],"PM2.5:3":row[76],"PM2.5:4":row[77],"PM2.5:5":row[78],"PM2.5:6":row[79],"PM2.5:7":row[80],"PM2.5:8":row[81],"PM2.5:9":row[82],
    "PM2.5:10":row[83],"PM2.5:11":row[84],"PM2.5:12":row[85],"PM2.5:13":row[86],"PM2.5:14":row[87],"PM2.5:15":row[88],"PM2.5:16":row[89],"PM2.5:17":row[90],"PM2.5:18":row[91],"PM2.5:19":row[92],
    "PM2.5:20":row[93],"PM2.5:21":row[94],"PM2.5:22":row[95],"PM2.5:23":row[96],
    "O3:0":row[97],"O3:1":row[98],"O3:2":row[99],"O3:3":row[100],"O3:4":row[101],"O3:5":row[102],"O3:6":row[103],"O3:7":row[104],"O3:8":row[105],"O3:9":row[106],
    "O3:10":row[107],"O3:11":row[108],"O3:12":row[109],"O3:13":row[110],"O3:14":row[111],"O3:15":row[112],"O3:16":row[113],"O3:17":row[114],"O3:18":row[115],"O3:19":row[116],
    "O3:20":row[117],"O3:21":row[118],"O3:22":row[119],"O3:23":row[120],
    "C6H6:0":row[121],"C6H6:1":row[122],"C6H6:2":row[123],"C6H6:3":row[124],"C6H6:4":row[125],"C6H6:5":row[126],"C6H6:6":row[127],"C6H6:7":row[128],"C6H6:8":row[129],"C6H6:9":row[130],
    "C6H6:10":row[131],"C6H6:11":row[132],"C6H6:12":row[133],"C6H6:13":row[134],"C6H6:14":row[135],"C6H6:15":row[136],"C6H6:16":row[137],"C6H6:17":row[138],"C6H6:18":row[139],"C6H6:19":row[140],
    "C6H6:20":row[141],"C6H6:21":row[142],"C6H6:22":row[143],"C6H6:23":row[144],
    "CO:0":row[145],"CO:1":row[146],"CO:2":row[147],"CO:3":row[148],"CO:4":row[149],"CO:5":row[150],"CO:6":row[151],"CO:7":row[152],"CO:8":row[153],"CO:9":row[154],
    "CO:10":row[155],"CO:11":row[156],"CO:12":row[157],"CO:13":row[158],"CO:14":row[159],"CO:15":row[160],"CO:16":row[161],"CO:17":row[162],"CO:18":row[163],"CO:19":row[164],
    "CO:20":row[165],"CO:21":row[166],"CO:22":row[167],"CO:23":row[168]},row[0])
    #batch.put(row[0], { "trafico:totalvehiculostunel": row[1], "trafico:totalvehiculoscalle30": row[2],
    #    "trafico:velocidadmediasuperficie": row[3], "trafico:velocidadmediatunel": row[4]}, "20160612")


# After everything has been defined, run the script.
conn, batch = connect_to_hbase()
#	print "Connect to HBase. table name: %s, batch size: %i" % (table_name, batch_size)

try:

    insert_row(batch, estacion)

    batch.send()
finally:
    # No matter what happens, close the file handle.
    conn.close()
