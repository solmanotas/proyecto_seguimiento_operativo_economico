# %%
#  """ import librerias para el Script """%%
import pandas as pd
import numpy as np
import os
import shutil
import time
import psycopg2
import cx_Oracle
import calendar
from sqlalchemy import create_engine
from conexionBD import delete_rows
from conexionBD import write_table
from datetime import datetime
from datetime import timedelta
from dateutil.relativedelta import relativedelta
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
initial = inicio_modulo(800,'car_tabla_seguimiento_os')

# %%

# Establecer la conexión a PCEnergy
conn = psycopg2.connect(dbname='****',
                        user='****',
                        password='****',
                        host='****',
                        port='****')


# %%
 #Entrada manual del período

# new_date=input('Ingresar PERÍODO: ')

# %%
#Definir fechas, formatos y fechas d descargue de archivos
now = datetime.now()
new_date = now - timedelta(days=1)
mes = int(new_date.strftime('%m'))
mes = mes - 1
new_date = now.strftime('%Y%m')
day = now.strftime('%d')

fecha_actual = now.strftime('%Y%m%d%H%M%S')
fecha_asi_vamos = now.strftime('%Y-%m-%d')
fecha_ami = (now - relativedelta(days=1)).strftime('%d_%m_%Y')
periodo_study = (now - relativedelta(months=1)).strftime('%Y%m')
periodo_2 = (now - relativedelta(months=2)).strftime('%Y%m')

# %%
query_os_md='''SELECT * 
                FROM inf_siprem."ORDENES_MD"
                WHERE "PERIODO"='{}'
            '''.format(new_date)

# %%
query_os_me='''SELECT * 
                FROM inf_siprem."ORDENES_ME"
                WHERE "PERIODO"='{}'
            '''.format(new_date)

# %%
df_md = pd.read_sql_query(query_os_md,conn)
df_md = df_md.drop_duplicates()


# %%
df_me = pd.read_sql_query(query_os_me,conn)
df_me = df_me.drop_duplicates()


# %%
df = pd.concat([df_md,df_me], axis=0)

# %%
dff = df[["NUMERO_OS", "NIC", "CONTRATA", "BRIGADA", "TECNICO", "F_GEN","FECHA_CIERRE","TIPO_OS", "COMENTARIO_1", "TIPO_CLIENTE", "DEPARTAMENTO", "MUNICIPIO", "LOCALIDAD","TIPO_REV_APA_EXIS","NUM_APA_INST", "MARCA_APA_INST", "ANOMALIAS", "ANOMALIA_1","ANOMALIA_2", "CIERRE_OBSERVACIONES", "PROGRAMA", "ANOMALIA", "COD_AV", "DESC_AV", "ESTADO_SERVICIO_OSF", "TIPO_SUSPENSION", "COMENTARIO_2", "NUM_CAMP", "TIPO_ORDEN", "PERIODO"]]

# %%
#Eliminar los datos del proceso de SCR
dff = dff[dff['PROGRAMA']!='SCR']

# %%
cx_Oracle.init_oracle_client(lib_dir=r"C:\Oracle\instantclient_21_6")

oracle_connection_string = ('oracle+cx_oracle://{username}:{password}@' +
                            cx_Oracle.makedsn('{hostname}', '{port}', service_name='{service_name}'))

connora= cx_Oracle.connect(user='****',
                            password='****',
                            dsn='****',
                            encoding='UTF-8')

# %%
# ASOCIAR DFF CON LAS CAMPAÑAS

query_campañas= "select * from fm_inv_plan"
df_campañas = pd.read_sql_query(query_campañas,connora)

# %%
df_campañas.rename(columns={'PLAN_ID': 'NUM_CAMP'}, inplace=True)

# %%
dff.fillna(0, inplace=True)

# %%
dff.replace('<NULL>',0, inplace=True)

# %%
dff.replace('', 0, inplace=True)

# %%
#cambiar a datos númericos el id del Plan

dff['NUM_CAMP'] = pd.to_numeric(dff['NUM_CAMP'], errors='coerce', downcast='integer')

# %%
#Cruce con id plan
dff = pd.merge(dff,df_campañas, how='left',left_on='NUM_CAMP',right_on='NUM_CAMP')

# %%
# Recategorizar los que no tienen número de campañas
condic1 = [
               (dff['PROGRAMA'] == 'PQR'),
                (dff['PROGRAMA'] == 'CAMPAÑA') & (dff['NUM_CAMP'] == 0),
                (dff['PROGRAMA'] == 'CAMPAÑA') & (dff['NUM_CAMP'] != 0),
                (dff['PROGRAMA'] == 'MEDIDA ESPECIAL') & (dff['NUM_CAMP'] == 0),
                (dff['PROGRAMA'] == 'MEDIDA ESPECIAL') & (dff['NUM_CAMP'] != 0)

            ]
choices1 = [
            'PQR','OTROS CAMPAÑAS MD','CAMPAÑAS MD','OTROS MEDIDA ESPECIAL','CAMPAÑAS ME'
            ]

dff['TIPO DE LÍNEA']=np.select(condic1, choices1,0)

# %%
#Crear columna de Irregularidad


cond = dff['ANOMALIA_2'] == 'Con irregularidad'
dff.loc[cond, 'ANOMALIAS'] = 'True'
cond = dff['ANOMALIA_2'] == 'Sin irregularidad'
dff.loc[cond, 'ANOMALIAS'] = 'False'


dff['ANOMALIAS'] = dff['ANOMALIAS'].astype('str')

# %%
#Crear columna de IRREGULARIDAD

dff['CON IRREGULARIDAD'] = np.nan

cond13 = (dff['ANOMALIAS'] == 'True')
dff.loc[cond13, 'CON IRREGULARIDAD'] = 1
cond14 = (dff['ANOMALIAS'] == 'False')
dff.loc[cond14, 'CON IRREGULARIDAD'] = 0

# %%
#Crear columna de NORM CON MEDIDA

dff['NORM MEDIDA'] = np.nan

cond15 = (dff['MARCA_APA_INST'] !=0)
dff.loc[cond15, 'NORM MEDIDA'] = 1
cond16 = (dff['MARCA_APA_INST'] == 0)
dff.loc[cond16, 'NORM MEDIDA'] = 0

# %%
dff['MARCA_APA_INST'] = dff['MARCA_APA_INST'].astype('str')

# %%
#Crear columna de TIPO DE MEDIDOR
condic2 = [
                (dff['MARCA_APA_INST'].str.contains('NANSE')),
                (~dff['MARCA_APA_INST'].str.contains('NANSE'))&(dff['MARCA_APA_INST']!='0'),
                (dff['MARCA_APA_INST']=='0'),                          
            ]
choices2 = [
            'NANSEN','CONVENCIONAL','No se instala Medidor'
            ]

dff['TIPO DE MEDIDOR INST']=np.select(condic2, choices2,0)

# %%
meses = {0:'ACCIONES_ENERO',
              1:'ACCIONES_FEBRERO',
              2: 'ACCIONES_MARZO',
              3: 'ACCIONES_ABRIL',
              4: 'ACCIONES_MAYO',
              5: 'ACCIONES_JUNIO',
              6: 'ACCIONES_JULIO',
              7: 'ACCIONES_AGOSTO',
              8: 'ACCIONES_SEPTIEMBRE',
              9: 'ACCIONES_OCTUBRE',
              10: 'ACCIONES_NOVIEMBRE',
              11: 'ACCIONES_DICIEMBRE'}

# %%
#Crear df de nomenclaturas para asignar codigo de campaña

df_nomenclaturas = pd.read_sql_query("""SELECT * FROM  inf_siprem."NOMENCLATURA_CAMPAÑAS"
                                           """, conn)
#df_nomenclaturas = df_nomenclaturas[["ID_PLAN_INV", "COD_PLAN_PERDIDAS", "COD_CONCEPTO_PLAN", "COD_TERRITORIO", "TERRITORIO", "COD_PLAN_BASE", "PLAN_AGRUPADO", "PLAN_BASE", "CAMPAÑA", "DIVISION_GG", "TIPO_MEDIDA", "GRUPO_GENERAL", "INTERES_GG", "PLAN_PERDIDAS", "DEFINICION", "ID_TIPO_PLAN_INV", "PLAN_INVESTIGACION", "CREADO", "ASOCIADO", "ACCIONES_ENERO", "ACCIONES_FEBRERO", "ACCIONES_MARZO", "ACCIONES_ABRIL", "ACCIONES_MAYO", "ACCIONES_JUNIO", "ACCIONES_JULIO", "ACCIONES_AGOSTO", "ACCIONES_SEPTIEMBRE", "ACCIONES_OCTUBRE", "ACCIONES_NOVIEMBRE", "ACCIONES_DICIEMBRE"]]
df_nomenclaturas.rename(columns={'ID_PLAN_INV':'NUM_CAMP', 'COD_PLAN_BASE':'CODIGO CAMP'}, inplace=True)
dff['NUM_CAMP'] = pd.to_numeric(dff['NUM_CAMP'] , errors='coerce')
df_nomenclaturas['NUM_CAMP'] = pd.to_numeric(df_nomenclaturas['NUM_CAMP'] , errors='coerce')

# %%
mes_acciones = meses[mes] 
df_nomenclaturas = df_nomenclaturas[["NUM_CAMP", "COD_PLAN_PERDIDAS", "COD_CONCEPTO_PLAN", "COD_TERRITORIO", "TERRITORIO", "CODIGO CAMP", "PLAN_AGRUPADO", "PLAN_BASE", "CAMPAÑA", "DIVISION_GG", "TIPO_MEDIDA", "GRUPO_GENERAL", "INTERES_GG", "PLAN_PERDIDAS", "ID_TIPO_PLAN_INV", "PLAN_INVESTIGACION", f"{mes_acciones}"]]
df_nomenclaturas.rename(columns={f"{mes_acciones}": "ACCIONES_META"},inplace=True)
df_nomenclaturas['ZONA'] = np.nan
cond = ((df_nomenclaturas['TERRITORIO'] == 'Guajira') | (df_nomenclaturas['TERRITORIO'] == 'Magdalena') )
df_nomenclaturas.loc[cond,'ZONA']= 'Zona Norte'
cond = ((df_nomenclaturas['TERRITORIO'] != 'Guajira') & (df_nomenclaturas['TERRITORIO'] != 'Magdalena') )
df_nomenclaturas.loc[cond,'ZONA']= 'Zona Atlantíco'

# %%
dff = pd.merge(dff, df_nomenclaturas, how= 'left', left_on='NUM_CAMP', right_on='NUM_CAMP')

# %%
dff.fillna(0, inplace=True)

# %%
# # ##Crear df clientes sin medidor (directos)

# df_directos = pd.read_sql_query("""SELECT * FROM  inf_siprem."BASE_DIRECTOS_CONGELADOS" """, conn)
# df_directos = df_directos[['NIC','ESTADO ']]
# df_directos.rename(columns={'ESTADO ':'ESTADO_BASE_DIRECTOS'}, inplace=True)
# dff = pd.merge(dff, df_directos, how= 'left', left_on='NIC', right_on='NIC')

# %%
# # ##Crear df clientes sin medidor (directos) para asignar campaña 

path = r'C:\Users\diego.ustariz\Downloads\Base Suministros Directos (Siprem) - 202307.xlsx'
df_directos = pd.read_excel(path, sheet_name='Datos2')
######################################################################################
df_directos = df_directos[['NIC','Estado']]
df_directos.rename(columns={'Estado':'Estado_Base Directos'}, inplace=True)
dff = pd.merge(dff, df_directos, how= 'left', left_on='NIC', right_on='NIC')

# %%
# Lista de nuevos nombres de Contratistas 

dic_contrat={'2E INGENIERIA MASIVOS - ATLANTICO SUR':'2E INGENIERIA',
             'DELTEC - ATLANTICO NORTE':'DELTEC ATN',
             'DELTEC - ATLANTICO SUR':'DELTEC ATS',
             'DELTEC AMI - ATLANTICO NORTE': 'DELTEC ATN',
             'DELTEC AMI - ATLANTICO SUR': 'DELTEC ATS',
             'DELTEC MEDESP - ATLANTICO NORTE': 'DELTEC ME ATN',
             'DELTEC MEDESP - ATLANTICO SUR': 'DELTEC ME ATS',
             'INELCIME MASIVOS - ATLANTICO NORTE':'INELCIME',
             'INELCIME  MASIVOS - ATLANTICO NORTE':'INELCIME',
             'INMEL - GUAJIRA MAICAO':'INMEL GUA',
             'INMEL - GUAJIRA RIOHACHA':'INMEL GUA',
             'INMEL - GUAJIRA SUR SAN JUAN':'INMEL GUA',
             'INMEL AMI - GUAJIRA':'INMEL GUA',
             'INMEL - MAGDALENA NORTE':'INMEL MAG',
             'INMEL - MAGDALENA SUR':'INMEL MAG',
             'INMEL AMI - MAGDALENA':'INMEL MAG',
             'INMEL MEDESP - GUAJIRA':'INMEL ME GUA',
             'INMEL MEDESP - MAGDALENA':'INMEL ME MAG',
             'SYM LOG MASIVOS - MAGDALENA':'S&M',
             'BRIGADAS ELITE':'BRIGADAS ELITE',
             'BRIGADAS ELITE MAGD':'BRIGADAS ELITE',
             'BRIGADAS ELITES MEDESP':'BRIGADAS ELITE',
             'BRIGADAS ELITES MEDESP MAGD GUAJ':'BRIGADAS ELITE',
             'BARRIDO MEDIA TENSION - BMT':'OTROS',
             'ELECTROMAR ASEG RED - MAGDALENA':'OTROS',
             'PROING ASEG RED - MAGDALENA':'OTROS'
}

# %%
dff['CONTRATA_ABREV']=dff['CONTRATA'].map(dic_contrat).fillna(dff['CONTRATA'])

# %%
# dff['CODIGO CAMP']= np.nan

# %%
#Crear listas contratistas para clasificación de OS 280

list_contrata_060_13 = ['DELTEC ATN','DELTEC ATS','INMEL GUA','INMEL MAG','BRIGADAS ELITE']
list_contrata_060_66 = ['2E INGENIERIA','INELCIME','S&M LOG','S&M','OTROS']

# %%
#Asignar Campaña de NOrmalización de Directos a tipo OS 280

#------Directos por Redes-------------

# Contratistas redes, cruza con directos de la base y normalización convencional o no hubo
condic4 = (dff['TIPO_ORDEN']=='TO280')&(dff['Estado_Base Directos']=='Directos ')&(dff['TIPO DE MEDIDOR INST']!='NANSEN')&(dff['CONTRATA_ABREV'].isin(list_contrata_060_66)&(dff['CODIGO CAMP']==0))           
dff.loc[condic4,'CODIGO CAMP'] = '010-190' 

# Contratistas redes, cruza con directos de la base y normalización NANSEN
condic5 = (dff['TIPO_ORDEN']=='TO280')&(dff['Estado_Base Directos']=='Directos ')&(dff['TIPO DE MEDIDOR INST']=='NANSEN')&(dff['CONTRATA_ABREV'].isin(list_contrata_060_66)&(dff['CODIGO CAMP']==0))              
dff.loc[condic5,'CODIGO CAMP'] = '060-66'


# Contratistas redes, cruza con normalizados/ no está en la base de directos  y normalización convencional o no hubo
condic6 = (dff['TIPO_ORDEN']=='TO280')&(dff['Estado_Base Directos']!='Directos ')&(dff['TIPO DE MEDIDOR INST']!='NANSEN')&(dff['CONTRATA_ABREV'].isin(list_contrata_060_66)&(dff['CODIGO CAMP']==0))          
dff.loc[condic6,'CODIGO CAMP'] = '010-189'


# Contratistas redes, cruza con normalizados/ no está en la base de directos  y normalización convencional o no hubo
condic7 = (dff['TIPO_ORDEN']=='TO280')&(dff['Estado_Base Directos']!='Directos ')&(dff['TIPO DE MEDIDOR INST']=='NANSEN')&(dff['CONTRATA_ABREV'].isin(list_contrata_060_66)&(dff['CODIGO CAMP']==0))          
dff.loc[condic7,'CODIGO CAMP'] = '010-15'


#-------Directos por Masivas----------------


# Contratistas masivas y cruza con directos de la base (no importa si hubo normalización)
condic3 = (dff['TIPO_ORDEN']=='TO280')&(dff['Estado_Base Directos']=='Directos ')&(dff['CONTRATA_ABREV'].isin(list_contrata_060_13)&(dff['CODIGO CAMP']==0))
dff.loc[condic3,'CODIGO CAMP'] = '060-13' 


# Contratistas masivas, cruza con normalizados/ no está en la base de directos
condic8 = (dff['TIPO_ORDEN']=='TO280')&(dff['Estado_Base Directos']!='Directos')&(dff['CONTRATA_ABREV'].isin(list_contrata_060_13)&(dff['CODIGO CAMP']==0))
dff.loc[condic8,'CODIGO CAMP'] = '060-02'


# %%
##Crear df Contratación de Ilegales para asignar campaña 

path = r'C:\Users\diego.ustariz\Downloads\Contrataciones Ilegales 2023.xlsx'
df_ilegales = pd.read_excel(path, sheet_name='Datos')
######################################################################################
df_ilegales = df_ilegales[['NIC','Estado']]
df_ilegales.rename(columns={'Estado':'Estado_Base Ilegales'}, inplace=True)
dff = pd.merge(dff, df_ilegales, how= 'left', left_on='NIC', right_on='NIC')

# %%
#Asignar Campaña de NOrmalización de Ilegales a tipo OS 280


# Contratistas Redes, cruza con normalizados/ no está en la base de ilegales
condic9 = (dff['TIPO_ORDEN']=='TO280')&(dff['Estado_Base Ilegales']!='Pendiente Normalización')&(dff['CONTRATA_ABREV'].isin(list_contrata_060_66))           
dff.loc[condic9,'CODIGO CAMP'] = '060-12'

# Contratistas Masivas, cruza con pendiente normalización
condic10 = (dff['TIPO_ORDEN']=='TO280')&(dff['Estado_Base Ilegales']=='Pendiente Normalización')&(dff['CONTRATA_ABREV'].isin(list_contrata_060_13))           
dff.loc[condic10,'CODIGO CAMP'] = '060-63'


# %%
df_territorio = pd.read_sql(""" SELECT "CUENTA" AS NIC, "TERRITORIO" AS TERRITORIAL FROM datafenix."CE_TABLA_GENERAL" """,conn)
df_territorio.columns = df_territorio.columns.str.upper() 

# %% Campañas que no estan asociada con los datos de las Nomenclaturas
# se crea dff_2 que son de la TO280 que no tiene informacion de la campaña para tratarla por separado
campañas_sin_asociar = ['010-15','010-189','010-190','060-02','060-12','060-13','060-63']
dff_2 = dff[(dff['TIPO_ORDEN']=='TO280') & (dff['CODIGO CAMP'].isin(campañas_sin_asociar))]
dff = dff[~dff['NUMERO_OS'].isin(dff_2['NUMERO_OS'])]

# %% Agregar  territorios a los que no tienen asociado o estan en Nan
dff_2 = pd.merge(dff_2,df_territorio,how='left', left_on='NIC',right_on='NIC')
dff_2['TERRITORIO'] = dff_2['TERRITORIAL'] 
del dff_2['TERRITORIAL'] 

# %%
# Territorios Vacios
atlantico_norte = ['BARRANQUILLA','SOLEDAD','PUERTO COLOMBIA']
cond = (dff_2['TERRITORIO'].isnull()) & (dff_2['DEPARTAMENTO']=='LA GUAJIRA')
dff_2.loc[cond,'TERRITORIO'] = 'Guajira'
cond = (dff_2['TERRITORIO'].isnull()) & (dff_2['DEPARTAMENTO']=='MAGDALENA')
dff_2.loc[cond,'TERRITORIO'] = 'Magdalena'
cond = (dff_2['TERRITORIO'].isnull()) & (dff_2['DEPARTAMENTO']=='ATLÁNTICO') & (dff_2['MUNICIPIO'].isin(atlantico_norte))
dff_2.loc[cond,'TERRITORIO'] = 'Atlántico Norte'
cond = (dff_2['TERRITORIO'].isnull()) & (dff_2['DEPARTAMENTO']=='ATLÁNTICO') & (~dff_2['MUNICIPIO'].isin(atlantico_norte))
dff_2.loc[cond,'TERRITORIO'] = 'Atlántico Sur'


# %%
dff_2 = dff_2[['NUMERO_OS',	'NIC',	'CONTRATA',	'BRIGADA',	'TECNICO',	'F_GEN',	'FECHA_CIERRE',	'TIPO_OS',	'COMENTARIO_1',
	'TIPO_CLIENTE',	'DEPARTAMENTO',	'MUNICIPIO',	'LOCALIDAD',	'TIPO_REV_APA_EXIS',	'NUM_APA_INST',	'MARCA_APA_INST',
    'ANOMALIAS',	'ANOMALIA_1',	'ANOMALIA_2',	'CIERRE_OBSERVACIONES',	'PROGRAMA',	'ANOMALIA',	'COD_AV',
    'DESC_AV',	'ESTADO_SERVICIO_OSF',	'TIPO_SUSPENSION',	'COMENTARIO_2', 'TIPO_ORDEN','PERIODO', 'NAME', 'TIPO DE LÍNEA',
    'CON IRREGULARIDAD', 'NORM MEDIDA',	'TIPO DE MEDIDOR INST', 'TERRITORIO', 'CODIGO CAMP', 'Estado_Base Directos',
    'CONTRATA_ABREV',	'Estado_Base Ilegales']]

# %%
dff_2['CODIGO_Z'] = dff_2['TERRITORIO'].map(str)+'-'+ dff_2['CODIGO CAMP'].map(str)

# %%
df_nomenclaturas['CODIGO_Z'] = df_nomenclaturas['TERRITORIO'].map(str)+'-'+ df_nomenclaturas['CODIGO CAMP'].map(str)

# %%
del dff_2['TERRITORIO'], dff_2['CODIGO CAMP']
dff_2 = pd.merge(dff_2,df_nomenclaturas, how='left', left_on='CODIGO_Z', right_on='CODIGO_Z')
del dff_2['CODIGO_Z']

# %%
dff = pd.concat([dff,dff_2])

# %%
#Viistas Fallidas

dff['VISITAS_FALLIDAS'] = np.nan


#df = df['VISITAS_FALLIDAS'].fillna('') 
cond11 = (dff['COD_AV'] == '')|(dff['TIPO_REV_APA_EXIS']=='Visita Fallida')|(dff['TIPO_REV_APA_EXIS']=='Resolución de escritorio')
dff.loc[cond11, 'VISITAS_FALLIDAS'] = 1
# cond12 = dff['VISITAS_FALLIDAS'].isnull()
# dff.loc[cond12, 'VISITAS_FALLIDAS'] = 0

# %%
dff.drop_duplicates('NUMERO_OS', inplace=True)

# %%
query = """ SELECT * FROM inf_siprem."ORDENES_MATERIALES" """
os_materiales = pd.read_sql_query(query,conn)


# %%
query = """ SELECT * FROM inf_siprem."PRECIO_MAT" """
precio_mat = pd.read_sql_query(query,conn)

# %%
precio_mat = precio_mat[["COD_MATERIAL", "PRECIO"]]
precio_mat.rename(columns={'COD_MATERIAL': 'COD_MAT'}, inplace = True)
precio_mat['COD_MAT'] = pd.to_numeric(precio_mat['COD_MAT'], errors='coerce')

# %%
precio_mat['COD_MAT'] = pd.to_numeric(precio_mat['COD_MAT'], errors='coerce')

# %%
df_materiales = pd.merge(os_materiales,precio_mat, how= 'left', left_on='COD_MAT',right_on='COD_MAT')

# %%
#Crear columna para calcular valor de material total

df_materiales['PRECIO_TOTAL_MATERIAL'] = df_materiales['CANTIDAD']*df_materiales['PRECIO']

# %%
df_mat = df_materiales.groupby(['ORDEN'])['PRECIO_TOTAL_MATERIAL'].sum().reset_index()

# %%
df_mat.rename(columns={'ORDEN':'NUMERO_OS'}, inplace=True)
dff = pd.merge(dff,df_mat, how= 'left', left_on='NUMERO_OS',right_on='NUMERO_OS')

# %%
#---------------------MANO DE OBRA---------------------------------------
# Consulta tablas de ordenes Mano de Obra y Precio Materiales
query = """ SELECT * FROM inf_siprem."ORDENES_MANO_OBRA" """
os_mano_obra = pd.read_sql_query(query,conn)
query = """ SELECT * FROM inf_siprem."PRECIO_MO" """
precio_mano_obra = pd.read_sql_query(query,conn)

# %%
# ELIMINAR ESPACIOS EN BLANCO
os_mano_obra['CONTRATA'] = os_mano_obra['CONTRATA'].str.strip()
os_mano_obra['TERRITORIO'] = os_mano_obra['TERRITORIO'].str.strip()


# %%
# CREAR CODIGO AV-CONTRATA-TERRITORIO
os_mano_obra['CODIGO'] = os_mano_obra['AV'].map(str)+ ' - ' + os_mano_obra['CONTRATA'].map(str)+ ' - ' + os_mano_obra['TERRITORIO'].map(str)

# %%
#TOTALIZAR PRECIOS DE MO
precio_mano_obra.rename(columns={'CONTRATISTA':'CONTRATA'}, inplace=True)
precio_mano_obra = precio_mano_obra[['CODIGO','PRECIOS UNITARIOS ']]
precio_mano_obra=precio_mano_obra.drop_duplicates()
precio_mano_obra.rename(columns={'PRECIOS UNITARIOS ':'PRECIO_MANO_OBRA'}, inplace=True)
os_mano_obra = pd.merge(os_mano_obra,precio_mano_obra, how= 'left', left_on='CODIGO',right_on='CODIGO')

# %%
df_mano_obra = os_mano_obra.groupby(['ORDEN'])['PRECIO_MANO_OBRA'].sum().reset_index() 

# %%
df_mano_obra.rename(columns={'ORDEN':'NUMERO_OS'}, inplace=True)

# %%
dff = pd.merge(dff,df_mano_obra, how= 'left', left_on='NUMERO_OS',right_on='NUMERO_OS')

# %%
dff['COSTO_TOTAL'] = dff['PRECIO_TOTAL_MATERIAL'] + dff['PRECIO_MANO_OBRA']

# %%
tarifas = pd.read_sql_query(f"""SELECT "NIC", "NIS_RAD", "TARIFA" 
                                FROM pcenergy."OP_DATOS_BASICOS_DIARIO"
                                WHERE "PERIODO" = '{new_date}'""",conn)

# %%
dff = pd.merge(dff,tarifas, how='left', left_on='NIC', right_on='NIC')

# %%
dff.to_excel(f'Seguimiento_Operativo_Financiero_{new_date}.xlsx', index=False)

# %%
try:
    delete_rows('inf_siprem."SEGUIMIENTO_OS"',new_date)
    write_table(dff,'inf_siprem."SEGUIMIENTO_OS"')
except Exception as ex:
    print('Error en el cargue de ordenes: ', ex)


try:
    ant = get_fecha_anterior(800,'car_tabla_seguimiento_os')
    ult_fech = ultima_fecha_ejecucion(800, 'car_tabla_seguimiento_os', today)
    resp = set_fecha_actual(800, 'car_tabla_seguimiento_os', ant)
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
    log = log_proceso('car_tabla_seguimiento_os',f_inicio_,f_final,'Diego Ustariz',ant_mes,des_mes,estado,mes_actual,tiempo_ejecucion,error,hoy)
except Exception as e:
    print('Error al cargar el Log: ', e)
   