#! /bin/bash
cd /home/borja/Documentos/tiempo
#wget http://api.openweathermap.org/data/2.5/forecast/city?id=6359304&APPID=b3620ffbc0da9f9012abef90179e559a
wget http://www.aemet.es/xml/municipios/localidad_28079.xml
nombre=$(date +"%Y%m%d_%H%M")
cp localidad_28079.xml $nombre.xml
mv $nombre.xml /home/borja/Documentos/tiempo/datos_tiempo/
rm localidad_28079.xml
cd /home/borja/Documentos/tiempo/datos_tiempo/
start-dfs.sh
hdfs dfs -put $nombre.xml /user/datostiempo
stop-dfs.sh
