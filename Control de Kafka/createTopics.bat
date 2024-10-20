@echo off
setlocal
echo Creando Topics

set KAFKA_SERVER=192.168.56.1:9092
set TOPICS=Customer2Central Central2Customer Taxi2Central Central2Taxi Status

for %%t in (%TOPICS%) do (
	"C:\Kafka\bin\windows\kafka-topics.bat" --create --topic %%t --bootstrap-server %KAFKA_SERVER%

)

endlocal
pause