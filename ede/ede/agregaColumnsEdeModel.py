# -*- coding: utf-8 -*-
from datetime import datetime
from pytz import timezone 
import os, io, sys
from sqlalchemy import create_engine, inspect
from time import time #importamos la función time para capturar tiempos

#----------------------------------------------------------------------------
# Realiza conexión con la BD.
#----------------------------------------------------------------------------
def main():
  _result = True
  secPhase = "BD en blanco solo con parámetros definidos por Enlaces-Mineduc"
  #path_to_DB_file = "./ede/ede/ceds-nds-v7_1_encryptedD3.db"
  path_to_DB_file = "./ede/ede/copia.db"
  try:
    engine = create_engine(f"sqlite+pysqlcipher://:{secPhase}@/{path_to_DB_file}?cipher=aes-256-cfb&kdf_iter=64000")    
    conn = engine.connect()

    inspector = inspect(engine)
    table_names = inspector.get_table_names()
    #matchers = ['RecordStartDateTime','RecordEndDateTime']
    matchers = ['RecordStartEndTime']
    for table_name in table_names:
      if(table_name.startswith('Ref') or table_name in ['sqlite_sequence']):
        #print(f"skip: {table_name}")
        continue
      else:
        column_items = inspector.get_columns(table_name)
        matching = [s for s in column_items if any(xs in s['name'] for xs in matchers)]
        if(matching):
          #print(f"Table:{table_name}")        
          #engine.execute(f'ALTER TABLE {table_name} add column RecordStartDateTime DATETIME;')
          #engine.execute(f'ALTER TABLE {table_name} add column RecordEndDateTime DATETIME;')
          print(f'ALTER TABLE {table_name} RENAME COLUMN RecordStartEndTime to RecordEndDateTime;')
          # print('\t'.join(n for n in column_items[0]))
          # for c in column_items:
          #   assert len(c) == len(column_items[0])
          #   print('\t'.join(str(c[n]) for n in c))

    # rows = conn.execute("PRAGMA table_info('Person');")
    # if(rows.returns_rows and rows.fetchone()):
    #   print(f"¿Cero o + Elementos?: {rows.returns_rows}")
    #   print(f"Type: {type(rows)}")
    #   print(f"Elements: {rows.fetchone()}")

  except Exception as e:
    print(f"NO se pudo ejecutar la consulta: {str(e)}")
    return False
  finally:
    conn.close()

  return True
  
if __name__== "__main__":
  tiempo_inicial = time()
  t_stamp = str(int(datetime.timestamp(datetime.now(timezone('Chile/Continental')))))
  print("Iniciando ejecución...")  
  main()
  print(f'Tiempo de ejecucion: {str(time() - tiempo_inicial)}')  