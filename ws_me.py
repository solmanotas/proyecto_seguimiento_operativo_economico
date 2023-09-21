import time
import zipfile
import os
from os import remove
from datetime import datetime
from datetime import timedelta
from dateutil.relativedelta import relativedelta
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium import webdriver
import chromedriver_autoinstaller
from selenium.webdriver.chrome.options import Options
from conexionBD_panel import set_fecha_actual
from conexionBD_panel import get_fecha_anterior 
from conexionBD_panel import inicio_modulo
from conexionBD_panel import ultima_fecha_ejecucion
from conexionBD_panel import error_modulo
from conexionBD_panel import  log_proceso
from dateutil.relativedelta import relativedelta

f_inicio_ = datetime.now()
ant_mes_ =  f_inicio_ - relativedelta(months=1)
des_mes_ =  f_inicio_ + relativedelta(months=1)
mes_actual = f_inicio_.strftime('%Y%m')
des_mes = des_mes_.strftime('%Y%m')
ant_mes = ant_mes_.strftime('%Y%m')
today = datetime.now()
hoy = today.strftime('%Y-%m-%d')
initial = inicio_modulo(505,'exp_ws_siprem_me')


try:
        
    url = "https://sipremsol.co/"
    now = datetime.now()
    day = now.strftime('%d')
    day = int(day)
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
    driver.find_element(By.LINK_TEXT,("Reporte OS Gestionadas ME")).click()
    driver.find_element(By.XPATH,("""//*[@id="layout"]/div/div/div/div/div[2]/form/div[1]/div[1]/div/div[1]""")).click()
    driver.find_element(By.XPATH,("""//*[@id="layout"]/div/div/div/div/div[2]/form/div[1]/div[1]/div/div[2]/div[2]/span/button[1]""")).click()
    time.sleep(2)
    driver.find_element(By.XPATH,("""//*[@id="layout"]/div/div/div/div/div[2]/form/div[1]/div[2]/p""")).click()
    driver.find_element(By.XPATH,("""//*[@id="layout"]/div/div/div/div/div[2]/form/div[2]/button""")).click()
    #c = driver.find_element_by_class_name("btn btn-success").click()

    esperar_descarga = True
  
    while esperar_descarga:
        
        dir = 'C:/Users/usrcspcene2/Downloads'
        contenido = os.listdir(dir)
        archivos = []
        for fichero in contenido:
            if os.path.isfile(os.path.join(dir, fichero)) and fichero.endswith('.xlsx'):
                archivos.append(fichero)

        for i in range(len(archivos)):
                
            if archivos[i].find('res_os_ad_') != -1:
                print("Archivo .zip descargado")
                esperar_descarga = False
            else:
                print("Esperando descarga del archivo .xlsx")
                time.sleep(20)
    #Descomprimir archivo descargado en la ruta Descargas del pc y llevarlo a la carpeta a procesar
    #shutil.unpack_archive(ruta_zip, r'C:\GESIP\Gestiones_Siprem\Descargas Siprem\SCR')
    #print("Archivo .csv generado")

    # if  day >= 20:
    #     time.sleep(500)
    # elif day >= 15: 
    #     time.sleep(350)
    # else:
    #     time.sleep(100)

    c = driver.find_element(By.XPATH,("""/html/body/div[1]/div/div/div/ul/li[4]/a""")).click()
    #c = driver.find_element_by_class_name("icon-signout").click()
    print('finalizo la ejecucion')
    driver.close()
    
    try:
        ant = get_fecha_anterior(505,'exp_ws_siprem_me')
        ult_fech = ultima_fecha_ejecucion(505, 'exp_ws_siprem_me', ant)
        resp = set_fecha_actual(505, 'exp_ws_siprem_me', today)
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
        log = log_proceso('exp_ws_siprem_me',f_inicio_,f_final,'Diego Ustariz',ant_mes,des_mes,estado,mes_actual,tiempo_ejecucion,error,hoy)
    except Exception as e:
        print('Error al cargar el Log: ', e)

except Exception as Ex:
    print('Error en el Proceso: ', Ex)
    error = error_modulo(505,'exp_ws_siprem_me')
                            
#<button type="submit" name="descargar" class="btn btn-success"><i class="icon-download-alt"></i> Descargar</button>

