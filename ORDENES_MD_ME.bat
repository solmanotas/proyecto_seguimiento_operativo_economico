@echo off
set FECHA=%date:/=-%
echo Proceso: ORDENES_MD_ME (Diario)
echo fecha: %date%
echo Hora de Inicio : %time%

cd ..
cd G:\Gestion_Tecnologica\01_PCEnergy\03_Codigos_Act_Informes\Ingeniero_8\Air-e
python3 "G:\Gestion_Tecnologica\01_PCEnergy\03_Codigos_Act_Informes\Ingeniero_8\Air-e\ordenes_MD_ME.py"


exit