@echo off
set FECHA=%date:/=-%
echo Proceso: Seguimiento Ordenes de Servicios
echo fecha: %date%
echo Hora de Inicio : %time%
echo Ejecutando ...

cd ..
cd G:\Gestion_Tecnologica\01_PCEnergy\03_Codigos_Act_Informes\Ingeniero_8\M2C
python3 "G:\Gestion_Tecnologica\01_PCEnergy\03_Codigos_Act_Informes\Ingeniero_8\M2C\Sgto_Operat_Economico.py"


exit