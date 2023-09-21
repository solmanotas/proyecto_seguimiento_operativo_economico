import time
import zipfile
import os
from os import remove
from datetime import datetime
from datetime import timedelta
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
import chromedriver_autoinstaller
from selenium.webdriver.chrome.options import Options
from conexionBD_panel import set_fecha_actual
from conexionBD_panel import get_fecha_anterior 
from conexionBD_panel import inicio_modulo
from conexionBD_panel import ultima_fecha_ejecucion
from conexionBD_panel import error_modulo
from conexionBD_panel import  log_proceso
import time
from datetime import datetime
from datetime import timedelta
from dateutil.relativedelta import relativedelta

f_inicio_ = datetime.now()
ant_mes_ =  f_inicio_ - relativedelta(months=1)
des_mes_ =  f_inicio_ + relativedelta(months=1)
mes_actual = f_inicio_.strftime('%Y%m')
des_mes = des_mes_.strftime('%Y%m')
ant_mes = ant_mes_.strftime('%Y%m')
today = datetime.now()
hoy = today.strftime('%Y-%m-%d')
initial = inicio_modulo(505,'exp_ws_siprem')


try:
        
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
    c = driver.find_element(By.LINK_TEXT,("Reporte OS Gestionadas Resumen")).click()
    c = driver.find_element(By.CLASS_NAME,("icon-download-alt")).click()
    c = driver.switch_to.window(driver.window_handles[1])
    c = driver.find_element("id","proceed-button").click()
    time.sleep(100)
    driver.close()
    c = driver.switch_to.window(driver.window_handles[0])

    c = driver.find_element(By.CLASS_NAME,("icon-search")).click()
    # c = driver.find_element_by_link_text(
    #                             "Reporte OS Gestionadas Resumen").click()
    c = driver.find_element(By.LINK_TEXT,("Reporte OS Gestionadas")).click()
    c = driver.find_element(By.CLASS_NAME,("icon-download-alt")).click()
    c = driver.switch_to.window(driver.window_handles[1])
    c = driver.find_element("id","proceed-button").click()
    
    esperar_descarga = True
  
    while esperar_descarga:
        
        dir = 'C:/Users/usrcspcene2/Downloads'
        contenido = os.listdir(dir)
        archivos = []
        for fichero in contenido:
            if os.path.isfile(os.path.join(dir, fichero)) and fichero.endswith('.zip'):
                archivos.append(fichero)

        for i in range(len(archivos)):
                
            if archivos[i].find('Consolidado_Especial_{}'.format(mes_actual)) != -1:
                print("Archivo .zip descargado")
                esperar_descarga = False
            else:
                print("Esperando descarga del archivo .zip")
                time.sleep(20)



    driver.close()
    c = driver.switch_to.window(driver.window_handles[0])

    c = driver.find_element(By.CLASS_NAME,("icon-signout")).click()
    driver.implicitly_wait(20)
    time.sleep(10)
    driver.close()

    now = datetime.now()
    new_date = now - timedelta(days=1)
    new_date = now.strftime('%Y%m')
    zif = r"""C:\Users\usrcspcene2\Downloads\Consolidado_Especial_%s.zip""" % (new_date)
    zif2 = r"""C:\Users\usrcspcene2\Downloads\Consolidado_%s.zip""" % (new_date)
    ruta_extraccion = r"""G:\Gestion_Tecnologica\01_PCEnergy\03_Codigos_Act_Informes\Ingeniero_8\M2C\Historico"""
    ruta_extraccion2 = r"""G:\Gestion_Tecnologica\01_PCEnergy\03_Codigos_Act_Informes\Ingeniero_8\M2C\Historico"""

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

    y = zipfile.ZipFile(zif2, "r")
    try:
        y.extractall(pwd=password, path=ruta_extraccion2)
    except AttributeError:
        print('Error')
        pass
    y.close()
    remove(zif2)

    try:
        ant = get_fecha_anterior(505,'exp_ws_siprem')
        ult_fech = ultima_fecha_ejecucion(505, 'exp_ws_siprem', ant)
        resp = set_fecha_actual(505, 'exp_ws_siprem', today)
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
        log = log_proceso('exp_ws_siprem',f_inicio_,f_final,'Diego Ustariz',ant_mes,des_mes,estado,mes_actual,tiempo_ejecucion,error,hoy)
    except Exception as e:
        print('Error al cargar el Log: ', e)
except Exception as Ex:
    print('Error en el Proceso: ', Ex)
    error = error_modulo(505,'exp_ws_siprem')
    
