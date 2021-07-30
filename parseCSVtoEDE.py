# -*- coding: utf-8 -*-
import logging

#import argparse #Librería usada en funcion main() para crear el menú de la consola
import importlib.util #utilizada en función module_from_file para cargar las librerías del estándar
from zipfile import ZipFile
import io, os, sys 
from datetime import datetime
from pytz import timezone 
from time import time #importamos la función time para capturar tiempos

def module_from_file(module_name, file_path):
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

def setup_custom_logger(name, t_stamp):
  _nameLogFile = f'./{t_stamp}_LOG.txt'
  stream_formatter = logging.Formatter(fmt='%(asctime)s - %(levelname)s - %(module)s - %(levelname)s - %(message)s')
  file_formatter=logging.Formatter(
    '{"time": "%(asctime)s", "name": "%(name)s", "module": "%(module)s", "funcName": "%(funcName)s", \
      "lineno": "%(lineno)d", "levelname": "%(levelname)s", "message": "%(message)s", "pathname": "%(pathname)s"}'
  )

  handlerStream = logging.StreamHandler(); 
  handlerStream.setFormatter(stream_formatter)
  
  handlerFile = logging.FileHandler(_nameLogFile, mode='w', encoding='utf8')
  handlerFile.setFormatter(file_formatter)

  logger = logging.getLogger(name)
  _logginLevel = logging.DEBUG if "--debug" in sys.argv else logging.INFO
  logger.setLevel(_logginLevel)
  logger.addHandler(handlerStream)
  logger.addHandler(handlerFile)
  return logger

def main():
  tiempo_inicial = time()
  t_stamp = str(int(datetime.timestamp(datetime.now(timezone('Chile/Continental')))))
  logger = setup_custom_logger('root', t_stamp)

  logger.info("Iniciando ejecución desde 'parseCSVtoEDE.py'...")

  if(len(sys.argv) < 2):
    sys.argv.append('--help')

  module = module_from_file("ede", "./ede/ede/consoleMenu.py")
  menu = module.consoleMenu()
  args = menu.parser.parse_args() #Obtiene la lista desde la consola

  args._encode = 'utf8' #Opciones Windows:'cp1252', Google Colab: 'utf8'
  args._sep = ';'       #Opciones Windows:';', Google Colab: ','
  args.path_to_dir_csv_file = './csv/'
  args.t_stamp = t_stamp

  if('func' in args): 
    args.func(args); #Ejecuta la función por defecto
  else:
    logger.info(parser.format_help())

  zipfileName = f'{t_stamp}_Data.zip'

  zipFile = ZipFile(f'./{zipfileName}','a')
  listFiles = [
    f'./{t_stamp}_key.txt',
    f'./{t_stamp}_key.encrypted',
    f'./{t_stamp}_ForenKeyErrors.csv',
    f'./{t_stamp}_LOG.txt',
    f'./{t_stamp}_encryptedD3.db'
    ] + [os.path.join(r,file) for r,d,f in os.walk("./csv") for file in f] 

  logger.info(f"Archivos a comprimir {listFiles}")

  for file in listFiles:
    if os.path.exists(file):
      if not file.endswith('_key.txt') and not file.startswith('./csv/'):
        zipFile.write(file)
      if file.startswith('./csv/') and "parse" in sys.argv:
        zipFile.write(file)
      if not file.startswith('./csv/'):
        os.remove(file)
  
  logger.info(f'Archivo creado con éxito: {zipfileName}')
  logger.info("Finalizando ejecución desde 'ede.py'...")
  logger.info(f'Tiempo de ejecucion: {str(time() - tiempo_inicial)}')
  
if __name__== "__main__":
  main()