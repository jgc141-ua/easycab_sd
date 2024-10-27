@echo off
setlocal
echo Instalando librerías...
set LIBINSTALL=colorama kafka-python

for %%t in (%LIBINSTALL%) do (
	pip install %%t

)

endlocal
pause