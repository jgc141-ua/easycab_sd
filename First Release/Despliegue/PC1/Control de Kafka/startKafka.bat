@echo off
echo Iniciando Zookeeper y Kafka...

start "Iniciando Zookeeper" /min cmd /k ""C:\Kafka\bin\windows\zookeeper-server-start.bat" "C:\Kafka\config\zookeeper.properties""
timeout /t 25 > nul

start "Iniciando Kafka" /min cmd /k ""C:\Kafka\bin\windows\kafka-server-start.bat" "C:\Kafka\config\server.properties""
timeout /t 25 > nul

pause