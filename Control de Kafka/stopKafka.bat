@echo off
echo Deteniendo Zookeeper y Kafka...

start "Deteniendo Kafka" /min cmd /k "C:\Kafka\bin\windows\kafka-server-stop.bat"
timeout /t 10 > nul

start "Deteniendo Zookeeper" /min cmd /k "C:\Kafka\bin\windows\zookeeper-server-stop.bat"
timeout /t 10 > nul

pause