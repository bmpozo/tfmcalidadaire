#! /bin/bash
cd /home/borja/Documentos/aire
wget http://www.mambiente.munimadrid.es/opendata/horario.txt
nombre=$(date +"%Y%m%d_%H%M")
cp horario.txt $nombre.txt
mv $nombre.txt /home/borja/Documentos/aire/datos_aire/
rm horario.txt
cd /home/borja/Documentos/aire/datos_aire/
#start-dfs.sh
hdfs dfs -put $nombre.txt /user/datosaire/sin_tratar
#stop-dfs.sh
