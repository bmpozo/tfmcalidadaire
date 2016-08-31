import pandas as pd
import numpy as np
import csv
import happybase
import time
#para ahorrar problemas problemas se declara como una constante en la que cambiamos el dia
DIA_CONSTANTE = '20160831'
#funcion para conectar con HBase
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

    return conn, table, batch

#funcion para escribir la funcion
def read_row(tabla, fila, columunfamily):
    return tabla.row(fila,columns=columunfamily)
#### ----- DATOS ----- ####
#### ZONA DE AIRE ####
#parametros de la conexion
batch_size = 1000
host = 'localhost'
namespace = "calidadaire"
row_count = 0
start_time = time.time()
table_name = "medicion_aire"
#variable que guardara toda la informacion del aire para meter en la tabla finall
fila_aire=[]
#abrimos conexion
conn, table, batch = connect_to_hbase()
print "Connect to HBase. table name: %s, batch size: %i" % (table_name, batch_size)
try:
    dia = DIA_CONSTANTE
    columnas = [['SO2'],['CO'],['NO2'],['PM2.5'],['PM10'],['BEN'],['O3']]
    #obtenemos 24 valores correspondientes a las 24 horas del dia
    for i in columnas:
            resultado = read_row(table, dia, i)
            batch.send()
            #resultado es un dict
            aux = resultado.values()
            aux = [float(j) for j in aux]
            fila_aire.append(sum(aux)/len(aux))
finally:
    # Cerramos conexion para que no se quede nada abierto
    conn.close()
#### FIN ZONA AIRE ####
#### ZONA DE TIEMPO ####
#parametros de la conexion
batch_size = 1000
host = 'localhost'
namespace = "calidadaire"
row_count = 0
start_time = time.time()
table_name = "medicion_tiempo"
#variable que guardara toda la informacion del tiempo para meter en la tabla finall
fila_tiempo=[]
#abrimos conexion
conn, table, batch = connect_to_hbase()
print "Connect to HBase. table name: %s, batch size: %i" % (table_name, batch_size)
try:
    dia = DIA_CONSTANTE
    columnas = ['temperatura_maxima','temperatura_minima','precipitaciones','viento']
    #obtenemos 7 datos en cada columnfamily correspondiente al dia actual y los 6 siguientes
    fila_tiempo = read_row(table, dia, columnas)
    batch.send()
finally:
    # Cerramos conexion para que no se quede nada abierto
    conn.close()
#### FIN ZONA TIEMPO ####
#### ZONA DE TRAFICO ####
#parametros de la conexion
batch_size = 1000
host = 'localhost'
namespace = "calidadaire"
row_count = 0
start_time = time.time()
table_name = "medicion_trafico"
#variable que guardara toda la informacion del trafico para meter en la tabla finall
fila_trafico=[]
#abrimos conexion
conn, table, batch = connect_to_hbase()
print "Connect to HBase. table name: %s, batch size: %i" % (table_name, batch_size)
try:
    dia = DIA_CONSTANTE
    columnas = [['total_vehiculos_tunel'],['total_vehiculos_calle30'],['velocidad_media_superficie'],['velocidad_media_tunel']]
    #obtenemos 24 datos correspondientes a las 24 horas del dia
    for i in columnas:
        resultado = read_row(table, dia, i)
        batch.send()
        #resultado es un dict por lo que tenemos que obtener la media de otra manera
        aux = resultado.values()
        aux = [float(j) for j in aux]
        fila_trafico.append(sum(aux)/len(aux))
finally:
    # Cerramos conexion para que no se quede nada abierto
    conn.close()
#print fila_aire
#print fila_tiempo
#print fila_trafico
#### FIN ZONA TRAFICO ####
#### ----- FIN DATOS ----- ####
#### ----- ANALITICA ----- ####
diccionario = dict()
#### ZONA DE AIRE ####
# 0 - buena, 1- admisible, 2- deficiente, 3 - mala
#PM10 -> 4
diccionario['PM10'] = fila_aire[4]
if (fila_aire[4] <= 50):
    diccionario['calidadPM10'] = 0
elif (50 < fila_aire[4] <= 90):
    diccionario['calidadPM10'] = 1
elif (90 < fila_aire[4] <= 150):
    diccionario['calidadPM10'] = 2
elif (150 < fila_aire[4]):
    diccionario['calidadPM10'] = 3
#SO2 -> 0
diccionario['SO2'] = fila_aire[0]
if (fila_aire[0] <= 175):
    diccionario['calidadSO2'] = 0
elif (175 < fila_aire[0] <= 350):
    diccionario['calidadSO2'] = 1
elif (350 < fila_aire[0] <= 525):
    diccionario['calidadSO2'] = 2
elif (525 < fila_aire[0]):
    diccionario['calidadSO2'] = 3
#NO2 -> 2
diccionario['NO2'] = fila_aire[2]
if (fila_aire[2] <= 100):
    diccionario['calidadNO2'] = 0
elif (100 < fila_aire[2] <= 200):
    diccionario['calidadNO2'] = 1
elif (200 < fila_aire[2] <= 300):
    diccionario['calidadNO2'] = 2
elif (300 < fila_aire[2]):
    diccionario['calidadNO2'] = 3
#CO -> 1
diccionario['CO'] = fila_aire[1]
if (fila_aire[1] <= 5):
    diccionario['calidadCO'] = 0
elif (5 < fila_aire[1] <= 10):
    diccionario['calidadCO'] = 1
elif (10 < fila_aire[1] <= 15):
    diccionario['calidadCO'] = 2
elif (15 < fila_aire[1]):
    diccionario['calidadCO'] = 3
#O3 -> 6
diccionario['O3'] = fila_aire[6]
if (fila_aire[6] <= 90):
    diccionario['calidadO3'] = 0
elif (90 < fila_aire[6] <= 180):
    diccionario['calidadO3'] = 1
elif (180 < fila_aire[6] <= 240):
    diccionario['calidadO3'] = 2
elif (240 < fila_aire[6]):
    diccionario['calidadO3'] = 3
#### FIN ZONA AIRE ####
#### ZONA DE TIEMPO ####
#{'temperatura_minima:1': '19', 'temperatura_minima:2': '20', 'temperatura_minima:3': '20', 'temperatura_minima:4': '20', 'temperatura_minima:5': '20', 'temperatura_minima:6': '20', 'temperatura_minima:7': '19',
#'precipitaciones:3': '40.0', 'precipitaciones:2': '0.0', 'precipitaciones:1': '0.0', 'precipitaciones:7': '0.0', 'precipitaciones:6': '0.0', 'precipitaciones:5': '0.0', 'precipitaciones:4': '20.0',
#'temperatura_maxima:2': '34', 'temperatura_maxima:3': '34', 'temperatura_maxima:1': '33', 'temperatura_maxima:6': '33', 'temperatura_maxima:7': '33', 'temperatura_maxima:4': '32', 'temperatura_maxima:5': '34',
#'viento:3': '5.0', 'viento:2': '15.0', 'viento:1': '10.0', 'viento:7': '20.0', 'viento:6': '15.0', 'viento:5': '20.0', 'viento:4': '20.0'}
auxtemp = (float(fila_tiempo.get('temperatura_maxima:1')) + float(fila_tiempo.get('temperatura_minima:1')))/2
diccionario['tempmax'] = float(fila_tiempo.get('temperatura_maxima:1'))
diccionario['tempmin'] = float(fila_tiempo.get('temperatura_minima:1'))
diccionario['temp'] = auxtemp
diccionario['prob_prec'] = float(fila_tiempo.get('precipitaciones:1'))
diccionario['viento']= float(fila_tiempo.get('viento:1'))
#para la temperatura tenemos que coger la media y ver su evolucion
tempsecuencia = [(float(fila_tiempo.get('temperatura_maxima:1'))+float(fila_tiempo.get('temperatura_minima:1')))/2,(float(fila_tiempo.get('temperatura_maxima:2'))+float(fila_tiempo.get('temperatura_minima:2')))/2,
(float(fila_tiempo.get('temperatura_maxima:3'))+float(fila_tiempo.get('temperatura_minima:3')))/2,(float(fila_tiempo.get('temperatura_maxima:4'))+float(fila_tiempo.get('temperatura_minima:4')))/2,
(float(fila_tiempo.get('temperatura_maxima:5'))+float(fila_tiempo.get('temperatura_minima:5')))/2,(float(fila_tiempo.get('temperatura_maxima:6'))+float(fila_tiempo.get('temperatura_minima:6')))/2,
(float(fila_tiempo.get('temperatura_maxima:7'))+float(fila_tiempo.get('temperatura_minima:7')))/2]
#guardamos el resultado de las distintas restas para luego directamente hacer un sum sobre el list
progresion = []
for l in range(len(tempsecuencia) - 1):
    progresion.append(tempsecuencia[l+1]-tempsecuencia[l])
#guardamos la suma total de l list para ver si crece,o decrece.
aux_progresion = sum(progresion)
if (aux_progresion > 2):
    diccionario['temp_cat'] = 1
if (-2 <= aux_progresion <= 2):
    diccionario['temp_cat'] = 0
if ( -2 > aux_progresion):
    diccionario['temp_cat'] = -1
#para la lluvia vamos a hacer ir mirando cada dia y ser si se clasifica como lluvia,,lluvia leve o no lluvia. Con esto obtener un resultado general con la misma escala
lluviasec = [float(fila_tiempo.get('precipitaciones:1')),float(fila_tiempo.get('precipitaciones:2')),float(fila_tiempo.get('precipitaciones:3')),float(fila_tiempo.get('precipitaciones:4')),float(fila_tiempo.get('precipitaciones:5')),float(fila_tiempo.get('precipitaciones:6')),float(fila_tiempo.get('precipitaciones:7'))]
info_lluvia = []
for k in range(len(lluviasec)):
    if (lluviasec[k] < 5):
        info_lluvia.append(1)
    if (5 <= lluviasec[k] < 15):
        info_lluvia.append(0)
    if (15 <= lluviasec[k]):
        info_lluvia.append(-1)
aux_info_lluvia = sum(info_lluvia)
if (aux_info_lluvia > 2):
    diccionario['prob_prec_cat'] = 1
if (-2 <= aux_info_lluvia <= 2):
    diccionario['prob_prec_cat'] = 0
if ( -2 > aux_info_lluvia):
    diccionario['prob_prec_cat'] = -1
#para el viento vamos a hacer ir mirando cada dia y ver si se claisifca como clamado,brisa suave,fuerte. Con esto obtener un resultado general con la misma escala
vientosec = [float(fila_tiempo.get('viento:1')),float(fila_tiempo.get('viento:2')),float(fila_tiempo.get('viento:3')),float(fila_tiempo.get('viento:4')),float(fila_tiempo.get('viento:5')),float(fila_tiempo.get('viento:6')),float(fila_tiempo.get('viento:7'))]
info_viento = []
for k in range(len(vientosec)):
    if (vientosec[k] < 5):
        info_viento.append(1)
    if (5 <= vientosec[k] < 15):
        info_viento.append(0)
    if (15 <= vientosec[k]):
        info_viento.append(-1)
aux_info_viento = sum(info_viento)
if (aux_info_viento > 2):
    diccionario['viento_cat'] = 1
if (-2 <= aux_info_viento <= 2):
    diccionario['viento_cat'] = 0
if ( -2 > aux_info_viento):
    diccionario['viento_cat'] = -1
#### FIN ZONA TIEMPO ####
#### ZONA DE TRAFICO ####
intensidad_media = (fila_trafico[0]+fila_trafico[1])/2
velocidad_media = (fila_trafico[2]+fila_trafico[3])/2
diccionario['vel_media'] = velocidad_media
#el valor medio de intensidad es 9900 pero es muy alto para los datos que tenemos de historicos pero este dato ya sabemos que apenas venia por lo que esto se deja indicado como se haria pero no se utiliza aun
#solo seria ver si intensidad_media esta por encima o por debajo del valor arriba indicado
if ((velocidad_media <= 40)|(velocidad_media >= 100)):
    diccionario['vel_media_cat'] = 1
else :
    diccionario['vel_media_cat'] = 0
#### FIN ZONA TRAFICO ####
#### ----- FIN ANALITICA ----- ####
#### ----- ENVIO A TABLA ----- ####
#solo falta generar el ultimo elemnto, la puntuacion final
aux1 = [diccionario['calidadPM10'],diccionario['calidadSO2'],diccionario['calidadNO2'],diccionario['calidadCO'],diccionario['calidadO3']]
diccionario['calidadaire'] = max(aux1)
diccionario['calidadfinal'] = diccionario['calidadaire'] + diccionario['temp_cat'] + diccionario['prob_prec_cat'] + diccionario['viento_cat'] + diccionario['vel_media_cat']
print "DICCIONARIO : ",diccionario
#conectamos con la tabla para meter la informacion
#parametros de la conexion
batch_size = 1000
host = 'localhost'
namespace = "calidadaire"
row_count = 0
start_time = time.time()
table_name = "tabla_final"
#abrimos conexion
conn, table, batch = connect_to_hbase()
print "Connect to HBase. table name: %s, batch size: %i" % (table_name, batch_size)
#funcion para meter los datos
def insert_row(batch, diction):
    batch.put(DIA_CONSTANTE,{'SO2:1':diction['SO2'],'calidadSO2:1':diction['calidadSO2'],'CO:1':diction['CO'],'calidadCO:1':diction['calidadCO'],'NO2:1':diction['NO2'],'calidadNO2:1':diction['calidadNO2'],
    'PM10:1':diction['PM10'],'calidadPM10:1':diction['calidadPM10'],'O3:1':diction['O3'],'calidadO3:1':diction['calidadO3'],'calidadaire:1':diction['calidadaire'],
    'temp:1':diction['temp'],'tempmax:1':diction['tempmax'],'tempmin:1':diction['tempmin'],'temp_cat:1':diction['temp_cat'],'prob_prec:1':diction['prob_prec'],'prob_prec_cat:1':diction['prob_prec_cat'],
    'viento:1':diction['viento'],'viento_cat:1':diction['viento_cat'],'vel_media:1':diction['vel_media'],'vel_media_cat:1':diction['vel_media_cat'],'calidadfinal:1':diction['calidadfinal']})
try:
    #tenemos en diccionario float y para meter en hbase tiene que ser str
    dicc_aux = dict()
    dicc_aux['SO2']=str(diccionario['SO2'])
    dicc_aux['calidadSO2']=str(diccionario['calidadSO2'])
    dicc_aux['CO']=str(diccionario['CO'])
    dicc_aux['calidadCO']=str(diccionario['calidadCO'])
    dicc_aux['NO2']=str(diccionario['NO2'])
    dicc_aux['calidadNO2']=str(diccionario['calidadNO2'])
    dicc_aux['PM10']=str(diccionario['PM10'])
    dicc_aux['calidadPM10']=str(diccionario['calidadPM10'])
    dicc_aux['O3']=str(diccionario['O3'])
    dicc_aux['calidadO3']=str(diccionario['calidadO3'])
    dicc_aux['calidadaire']=str(diccionario['calidadaire'])
    dicc_aux['temp']=str(diccionario['temp'])
    dicc_aux['tempmax']=str(diccionario['tempmax'])
    dicc_aux['tempmin']=str(diccionario['tempmin'])
    dicc_aux['temp_cat']=str(diccionario['temp_cat'])
    dicc_aux['prob_prec']=str(diccionario['prob_prec'])
    dicc_aux['prob_prec_cat']=str(diccionario['prob_prec_cat'])
    dicc_aux['viento']=str(diccionario['viento'])
    dicc_aux['viento_cat']=str(diccionario['viento_cat'])
    dicc_aux['vel_media']=str(diccionario['vel_media'])
    dicc_aux['vel_media_cat']=str(diccionario['vel_media_cat'])
    dicc_aux['calidadfinal']=str(diccionario['calidadfinal'])
    insert_row(batch, dicc_aux)
    batch.send()
finally:
    # No matter what happens, close the file handle.
    conn.close()
#### ----- FIN ENVIO A TABLA ----- ####
