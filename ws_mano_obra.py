import time
import zipfile
import os
import pandas as pd
from os import remove
from conexionBD import run_query, write_table, delete_rows
from datetime import datetime
from datetime import timedelta
from dateutil.relativedelta import relativedelta
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium import webdriver
import chromedriver_autoinstaller
from selenium.webdriver.chrome.options import Options
from conexionBD import run_query, delete_rows, write_table, delete_rows_all
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
initial = inicio_modulo(800,'ws_ordenes_Mano_Obra')


url = "https://sipremsol.co/"
now = datetime.now()
day = now.strftime('%d')
day = int(day)
fecha = now.strftime('%Y_%m_%d')
periodo = now.strftime('%Y%m')

# DRIVER_PATH = r"C:\MM2C\ChromeDriver\chromedriver.exe"
# option = webdriver.ChromeOptions()
# option.add_argument("--headless")
# driver = webdriver.Chrome(DRIVER_PATH, options=option)


chromedriver_autoinstaller.install() 
driver = webdriver.Chrome()
#driver = webdriver.Chrome(
#            executable_path=r"C:\Geckodriver\chromedriver.exe")
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

driver.find_element(By.CLASS_NAME,("icon-search")).click()
driver.find_element(By.LINK_TEXT,("Informe De Cierre Ordenes De Servicio Todos")).click()
driver.find_element(By.XPATH,("""//*[@id="layout"]/div/div/div/div/div[2]/form/table[1]/tbody/tr/td[1]/div/div/div""")).click()
driver.find_element(By.XPATH,("""//*[@id="layout"]/div/div/div/div/div[2]/form/table[1]/tbody/tr/td[1]/div/div/div/div[2]/div[2]/span/button[1]""")).click()
driver.find_element(By.XPATH,("""//*[@id="layout"]/div/div/div/div/div[2]/form/table[1]/tbody/tr/td[1]/div/div/div""")).click()
driver.find_element(By.XPATH,("""//*[@id="layout"]/div/div/div/div/div[2]/form/div/button""")).click()


esperar_descarga = True

while esperar_descarga:
    
    dir = 'C:/Users/usrcspcene2/Downloads'
    contenido = os.listdir(dir)
    archivos = []
    for fichero in contenido:
        if os.path.isfile(os.path.join(dir, fichero)) and fichero.endswith('.zip'):
            archivos.append(fichero)

    for i in range(len(archivos)):
            
        if archivos[i].find('Informe_Cierre_OS_ALIADOS_1062397422_{}'.format(fecha)) != -1:
            print("Archivo .zip descargado")
            esperar_descarga = False
        else:
            print("Esperando descarga del archivo .zip")
            time.sleep(20)

#driver.find_element(By.XPATH,("""//*[@id="layout"]/div/div/div/div/div[2]/form/table[1]/tbody/tr/td[1]/div/div/div""")).click()
driver.find_element(By.XPATH,("""/html/body/div[1]/div/div/div/ul/li[4]/a""")).click()

zif = r"C:\Users\usrcspcene2\Downloads\Informe_Cierre_OS_ALIADOS_1062397422_%s.zip" % (fecha)
ruta_extraccion = r"""G:\Gestion_Tecnologica\01_PCEnergy\03_Codigos_Act_Informes\Ingeniero_8\M2C\Historico"""

password = None

# open and extract all files in the zip
z = zipfile.ZipFile(zif, "r")
try:
    z.extractall(pwd=password, path=ruta_extraccion)
except AttributeError:
    print('Error')
    pass
z.close()
remove(zif)


old_archivo = r"G:\Gestion_Tecnologica\01_PCEnergy\03_Codigos_Act_Informes\Ingeniero_8\M2C\Historico\Informe_Cierre_OS_Todos_Aliados_1062397422_{}.csv".format(periodo)
try:
    os.remove(old_archivo)
except Exception as Ex:
    print('El archivo old no existe')
archivo = r'G:\Gestion_Tecnologica\01_PCEnergy\03_Codigos_Act_Informes\Ingeniero_8\M2C\Historico\Informe_Cierre_OS_Todos_Aliados_1062397422_{}.csv'.format(fecha)
new_archivo = r"G:\Gestion_Tecnologica\01_PCEnergy\03_Codigos_Act_Informes\Ingeniero_8\M2C\Historico\Informe_Cierre_OS_Todos_Aliados_1062397422_{}.csv".format(periodo)
os.rename(archivo,new_archivo)
try:
    os.remove(archivo)
except Exception as ex:
    print('el sistema no encuentra el archivo')

path = r"G:\Gestion_Tecnologica\01_PCEnergy\03_Codigos_Act_Informes\Ingeniero_8\M2C\Historico\Informe_Cierre_OS_Todos_Aliados_1062397422_{}.csv".format(periodo)
df_MO = pd.read_csv(path,sep=';',header=3, encoding='ISO-8859-1')
df_MO.columns = df_MO.columns.str.upper()
df_MO.rename(columns={'ALIADO':'CONTRATA','TIPO PROCESO':'PROGRAMA'}, inplace=True)

try:
    ant = get_fecha_anterior(800,'ws_ordenes_Mano_Obra')
    ult_fech = ultima_fecha_ejecucion(800, 'ws_ordenes_Mano_Obra', today)
    resp = set_fecha_actual(800, 'ws_ordenes_Mano_Obra', ant)
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
    log = log_proceso('ws_ordenes_Mano_Obra',f_inicio_,f_final,'Diego Ustariz',ant_mes,des_mes,estado,mes_actual,tiempo_ejecucion,error,hoy)
except Exception as e:
    print('Error al cargar el Log: ', e)


