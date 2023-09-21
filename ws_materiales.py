import time
import zipfile
import shutil
import os
import pandas as pd
from conexionBD import run_query, write_table, delete_rows
from os import remove
from datetime import datetime
from datetime import timedelta
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium import webdriver
import chromedriver_autoinstaller
from dateutil.relativedelta import relativedelta
from selenium.webdriver.chrome.options import Options
from conexionBD_panel import set_fecha_actual
from conexionBD_panel import get_fecha_anterior 
from conexionBD_panel import inicio_modulo
from conexionBD_panel import ultima_fecha_ejecucion
from conexionBD_panel import error_modulo
from conexionBD_panel import  log_proceso


f_inicio_ = datetime.now()
ant_mes_ =  f_inicio_ - relativedelta(months=1)
des_mes_ =  f_inicio_ + relativedelta(months=1)
mes_actual = f_inicio_.strftime('%Y%m')
des_mes = des_mes_.strftime('%Y%m')
ant_mes = ant_mes_.strftime('%Y%m')
today = datetime.now()
hoy = today.strftime('%Y-%m-%d')
initial = inicio_modulo(800,'ws_ordenes_Materiales')



hoy = datetime.now()
periodo = hoy.strftime('%Y%m')


url = "https://sipremsol.co/"


# gives an implicit wait for 20 seconds
# url = "https://sipremsol.co/"
# web_options = webdriver.ChromeOptions()
# web_options.add_argument('--headless')
# web_options.add_argument('--disable-gpu')
chromedriver_autoinstaller.install() 

driver = webdriver.Chrome()


#driver = webdriver.Chrome(
#    executable_path=r"C:\Geckodriver\chromedriver.exe")
## For maximizing window
driver.maximize_window()
driver.get(url)
user = driver.find_element("id","username")
user.send_keys("1062397422")
user.send_keys(Keys.ENTER)
password = driver.find_element("id","password")
password.send_keys("7422")
password.send_keys(Keys.ENTER)
cod_company = driver.find_element("id","codempresa")
cod_company.send_keys("2210")
cod_company.send_keys(Keys.ENTER)

c = driver.find_element(By.CLASS_NAME,"icon-search").click()
# c = driver.find_element_by_link_text(
#                             "Reporte OS Gestionadas Resumen").click()
c = driver.find_element(By.LINK_TEXT,("Reporte Consumo de Materiales Todos")).click()
c = driver.find_element(By.XPATH,("""//*[@id="layout"]/div/div/div/div/div[2]/div/div/div/div/div/div/div[2]/div[1]/div/div[3]/div/form/div[5]/button""")).click()
#c = driver.find_element(By.CLASS_NAME,("btn btn-success")).click()
esperar_descarga = True

while esperar_descarga:
    
    dir = 'C:/Users/usrcspcene2/Downloads'
    contenido = os.listdir(dir)
    archivos = []
    for fichero in contenido:
        if os.path.isfile(os.path.join(dir, fichero)) and fichero.endswith('.xlsx'):
            archivos.append(fichero)

    for i in range(len(archivos)):
            
        if archivos[i].find('Materiales_Medida_Todos_CaribeSOL_') != -1:
            print("Archivo Excel Materiales descargado")
            esperar_descarga = False
        else:
            print("Esperando descarga del archivo .xlsx")
            time.sleep(10)


            
c = driver.find_element(By.XPATH,("""/html/body/div[1]/div/div/div/ul/li[4]/a""")).click()
driver.close()

old_archivo = r"G:\Gestion_Tecnologica\01_PCEnergy\03_Codigos_Act_Informes\Ingeniero_8\M2C\Historico\Materiales_Medida_Todos_CaribeSOL_{}.xlsx".format(periodo)
try:
    os.remove(old_archivo)
except Exception as Ex:
    print('El archivo old no existe')

archivo = r'C:\Users\usrcspcene2\Downloads\Materiales_Medida_Todos_CaribeSOL_1062397422.xlsx'
new_archivo = r"C:\Users\usrcspcene2\Downloads\Materiales_Medida_Todos_CaribeSOL_{}.xlsx".format(periodo)
path_destino = r'G:\Gestion_Tecnologica\01_PCEnergy\03_Codigos_Act_Informes\Ingeniero_8\M2C\Historico'
os.rename(archivo,new_archivo)
shutil.move(new_archivo, path_destino)

path = r"G:\Gestion_Tecnologica\01_PCEnergy\03_Codigos_Act_Informes\Ingeniero_8\M2C\Historico\Materiales_Medida_Todos_CaribeSOL_{}.xlsx".format(periodo)
df = pd.read_excel(path, header=4)

try:
    ant = get_fecha_anterior(800,'ws_ordenes_Materiales')
    ult_fech = ultima_fecha_ejecucion(800, 'ws_ordenes_Materiales', today)
    resp = set_fecha_actual(800, 'ws_ordenes_Materiales', ant)
    print ('Proceso Ok')
    estado = 'Proceso Ejecutado OK'
    error = None
except Exception as Ex:
    error = str(Ex)
    print('Error al actualizar fecha: ', Ex)
    estado = 'Proceso No se Ejecuto Completamente'
print("termino la ejecucion")
f_final = datetime.now()
tiempo_ejecucion = f_final - f_inicio_

try:
    log = log_proceso('ws_ordenes_Materiales',f_inicio_,f_final,'Diego Ustariz',ant_mes,des_mes,estado,mes_actual,tiempo_ejecucion,error,hoy)
except Exception as e:
    print('Error al cargar el Log: ', e)