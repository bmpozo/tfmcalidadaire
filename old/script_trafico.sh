#! /bin/bash
cd /home/borja/Documentos/trafico
wget http://www.mc30.es/images/xml/DatosTrafico.xml
nombre=$(date +"%Y%m%d_%H%M")
cp DatosTrafico.xml $nombre.xml
mv $nombre.xml /home/borja/Documentos/trafico/datos_trafico/
rm DatosTrafico.xml
cd /home/borja/Documentos/trafico/datos_trafico/
start-dfs.sh
hdfs dfs -put $nombre.xml /user/datostrafico
stop-dfs.sh
