# -*- coding: utf-8 -*-
from datetime import datetime
from pytz import timezone 
import os, io, sys
from sqlalchemy import create_engine, inspect
from time import time #importamos la función time para capturar tiempos
import pandas as pd

#Carga planilla
def cargarPlanilla():
  url = './ede/ede/NDS-Reference-v7_1.xlsx';
  xd = pd.read_excel(url,'NDS Columns')
  _t=f'Planilla {url} cargada satisfactoriamente'; print(_t)
  return xd

#Inspecciona la estructura de la BD
def inspectDB():
  secPhase = "BD en blanco solo con parámetros definidos por Enlaces-Mineduc"
  #path_to_DB_file = "./ede/ede/ceds-nds-v7_1_encryptedD3.db"
  path_to_DB_file = "./ede/ede/copia.db"
  try:
    engine = create_engine(f"sqlite+pysqlcipher://:{secPhase}@/{path_to_DB_file}?cipher=aes-256-cfb&kdf_iter=64000")    
    return inspect(engine)
  except Exception as e:
    print(f"NO se pudo inspeccionar la BD: {str(e)}")
    return False
  

#----------------------------------------------------------------------------
# Realiza conexión con la BD.
#----------------------------------------------------------------------------
def main():
  _result = True
  inspector = inspectDB()
  xd = cargarPlanilla()
  
  table_names = inspector.get_table_names()
  #matchers = ['RecordStartDateTime','RecordEndDateTime']
  for table_name in table_names:
    if(table_name.startswith('Ref') or table_name in ['sqlite_sequence','_CEDSElements','_CEDStoNDSMapping','tmp']):
      continue
    else:
      #print(f"\n\nTable:{table_name}\n\n")          
      column_items = inspector.get_columns(table_name)
      #matching = [s for s in column_items if any(xs in s['name'] for xs in matchers)]
      #print('\t'.join(n for n in column_items[0]))
      for c in column_items:
        assert len(c) == len(column_items[0])
        _d = xd[(xd['Table']==table_name) & (xd['Column']==c['name'])].values
        if(not _d.any()):
          #print(table_name,c)
          print(table_name,'\t'.join(str(c[n]) for n in c))
        
        
  return _result
  
if __name__== "__main__":
  tiempo_inicial = time()
  t_stamp = str(int(datetime.timestamp(datetime.now(timezone('Chile/Continental')))))
  print("Iniciando ejecución...")  
  main()
  print(f'Tiempo de ejecucion: {str(time() - tiempo_inicial)}')  