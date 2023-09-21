@echo off
set FECHA=%date:/=-%
echo Proceso: Web Scrappy Materiales
echo fecha: %date%
echo Hora de Inicio : %time%

cd ..
cd G:\Gestion_Tecnologica\01_PCEnergy\03_Codigos_Act_Informes\Ingeniero_8\M2C
python3 "G:\Gestion_Tecnologica\01_PCEnergy\03_Codigos_Act_Informes\Ingeniero_8\M2C\ws_materiales.py"


exit