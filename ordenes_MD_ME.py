# %%
import pandas as pd
import numpy as np
import time
from datetime import datetime
from datetime import timedelta
from dateutil.relativedelta import relativedelta
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
initial = inicio_modulo(800,'exp_cargue_ordenes_md_me')

# %%
now = datetime.now()
now = now - timedelta(days=1)
periodo = now.strftime('%Y%m')

# %%
path_siprem = r"G:\Gestion_Tecnologica\01_PCEnergy\03_Codigos_Act_Informes\Ingeniero_8\M2C\Historico\Consolidado_Especial_%s.csv" % (periodo)
path_siprem_total = r"G:\Gestion_Tecnologica\01_PCEnergy\03_Codigos_Act_Informes\Ingeniero_8\M2C\Historico\Consolidado_%s.csv" % (periodo)
df_siprem_total = pd.read_csv(path_siprem_total, sep=';', encoding='ISO-8859-1', low_memory=False, index_col=False)
df_siprem_total = df_siprem_total[[ 'NUMERO_OS','COMENTARIO_2', 'NUM_CAMP', 'TIPO_ORDEN']]
df_siprem = pd.read_csv(path_siprem, sep=';', encoding='ISO-8859-1', low_memory=False)
df_siprem = pd.merge(df_siprem,df_siprem_total, how='left', left_on='NUMERO_OS', right_on='NUMERO_OS')
df_siprem.drop_duplicates('NUMERO_OS', keep='last', inplace=True)
df_siprem = df_siprem.fillna('')
df_siprem['PERIODO'] = periodo

# %%
try:
    delete_rows('inf_siprem."ORDENES_MD"', periodo)
    write_table(df_siprem, 'inf_siprem."ORDENES_MD"')
except Exception as Ex:
    print('Error al momento de realizar el Cargue: ',Ex)

# %%
path_m_especial = r"G:\Gestion_Tecnologica\01_PCEnergy\03_Codigos_Act_Informes\Ingeniero_8\M2C\Historico\Reporte_Medida_Especial_%s.xlsx" % (periodo)
# MEDIDA ESPECIAL
df_me = pd.read_excel(path_m_especial, header=4, index_col=False)
df_me.rename(columns={'NIS': 'NIS_RAD'}, inplace=True)
df_me.rename(columns={'ANOMALIA': 'ANOMALIA_2','NUM_CAMPANA':'NUM_CAMP'}, inplace=True)
#df_me = df_me[df_me['TIPO_REV_APA_EXIS'] != 'Visita Fallida']
df_me['PROGRAMA'] = 'Medida Especial'
df_me.drop_duplicates('NUMERO_OS', keep='last', inplace=True)
df_me = df_me[(df_me['TIPO_OS'].str.contains('TO803')) | (df_me['TIPO_OS'].str.contains('TO808'))|(~df_me['NUM_CAMP'].isnull())]
df_me['PERIODO'] = periodo


# %%
try:
    delete_rows('inf_siprem."ORDENES_ME"', periodo)
    write_table(df_me, 'inf_siprem."ORDENES_ME"')
except Exception as Ex:
    print('Error al momento de realizar el Cargue: ',Ex)


try:
    ant = get_fecha_anterior(800,'exp_cargue_ordenes_md_me')
    ult_fech = ultima_fecha_ejecucion(800, 'exp_cargue_ordenes_md_me', today)
    resp = set_fecha_actual(800, 'exp_cargue_ordenes_md_me', ant)
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
    log = log_proceso('exp_cargue_ordenes_md_me',f_inicio_,f_final,'Diego Ustariz',ant_mes,des_mes,estado,mes_actual,tiempo_ejecucion,error,hoy)
except Exception as e:
    print('Error al cargar el Log: ', e)
