# -*- coding: utf-8 -*-
import logging
logger = logging.getLogger('root');

#import argparse #Librería usada en funcion main() para crear el menú de la consola
import importlib.util #utilizada en función module_from_file para cargar las librerías del estándar
from zipfile import ZipFile
import io, os, sys 
from datetime import datetime
from pytz import timezone 
from time import time #importamos la función time para capturar tiempos
import requests, http
#import hashlib 

def main(**params):
  tiempo_inicial = time();
  t_stamp = str(int(datetime.timestamp(datetime.now(timezone('Chile/Continental')))));
  logger = setup_custom_logger('root', t_stamp);

  logger.info("Iniciando ejecución desde 'ede.py'...");

  #if("--help" not in sys.argv and "-h" not in sys.argv):
  code = updateFiles();
  code.execute();

  module = module_from_file("ede", "./ede/ede/consoleMenu.py");
  menu = module.consoleMenu();
  args = menu.parser.parse_args() #Obtiene la lista desde la consola

  args._encode = 'utf8' #Opciones Windows:'cp1252', Google Colab: 'utf8'
  args._sep = ';'       #Opciones Windows:';', Google Colab: ','
  args.path_to_dir_csv_file = './csv/';
  args.t_stamp = t_stamp;

  if('func' in args): 
    args.func(args); #Ejecuta la función por defecto
  else:
    logger.info(parser.format_help())
    
  zipFile = ZipFile(f'./{t_stamp}_Data.zip','a');
  listFiles = [
    f'./{t_stamp}_key.txt',
    f'./{t_stamp}_key.encrypted',
    f'./{t_stamp}_ForenKeyErrors.csv',
    f'./{t_stamp}_LOG.txt',
    f'./{t_stamp}_encryptedD3.db'
    ] + [os.path.join(r,file) for r,d,f in os.walk("./csv") for file in f] 

  logger.info(f"Archivos a comprimir {listFiles}");

  for file in listFiles:
    if os.path.exists(file):
      if not file.endswith('_key.txt') and not file.startswith('./csv/'):
        zipFile.write(file);
      if file.startswith('./csv/') and "parse" in sys.argv:
        zipFile.write(file);
      if not file.startswith('./csv/'):
        os.remove(file);

  logger.info("Finalizando ejecución desde 'ede.py'...");
  logger.info(f'Tiempo de ejecucion: {str(time() - tiempo_inicial)}');
  
if __name__== "__main__":
  main();  

def module_from_file(module_name, file_path):
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

def setup_custom_logger(name, t_stamp):
  _nameLogFile = f'./{t_stamp}_LOG.txt';
  stream_formatter = logging.Formatter(fmt='%(asctime)s - %(levelname)s - %(module)s - %(levelname)s - %(message)s');
  file_formatter=logging.Formatter(
    '{"time": "%(asctime)s", "name": "%(name)s", "module": "%(module)s", "funcName": "%(funcName)s", \
      "lineno": "%(lineno)d", "levelname": "%(levelname)s", "message": "%(message)s", "pathname": "%(pathname)s"}'
  )

  handlerStream = logging.StreamHandler(); 
  handlerStream.setFormatter(stream_formatter);
  
  handlerFile = logging.FileHandler(_nameLogFile, mode='w', encoding='utf8');
  handlerFile.setFormatter(file_formatter);

  logger = logging.getLogger(name)
  _logginLevel = logging.DEBUG if "--debug" in sys.argv else logging.INFO;
  logger.setLevel(_logginLevel);
  logger.addHandler(handlerStream)
  logger.addHandler(handlerFile)
  return logger

class updateFiles:
  def __init__(self, args=None):
    self.args = args;
    logger.info(f"typo de argumento: {type(self.args)}, valores: {self.args}");    
    self.filesDict = {
        '14Xz4t-zHZ70tGxWTk0IgbaDWTDgGmZgq': './ede/ede/consoleMenu.py',
        '1hb-Az7PHApRzq8GTRKpM_OibLzxM2ut_': './ede/ede/insertCSVtoSQLite.py',
        '1TuyK9XijM9n5Spc6iYcEKH__wsDS0Oxe': './ede/ede/parseJSONtoCSV.py',
        '1OOVCSRBN_UWHQAGxodlps_ftmA_PCNFN': './ede/ede/parseXLSXtoCSV.py',
        '1Y8cXxfMB53IWQ_C8dFtTZ-s7uUC8DoIQ': './ede/ede/checkSQLiteEDE.py',
        '1nSHe-bLtwSsNeNE1nlZK2VrQK2DeU9a9': 'parseCSVtoEDE.py',
        '1gBhiRXswoCN6Ub2PHtyr5cb6w9VMQgll': './ede/ede/NDS-Reference-v7_1.xlsx',
        '17c4tqJI7qUx0G_yDykV5ftAHj3Qq8ypA': './ede/ede/ceds-nds-v7_1_encryptedD3.db',
        '18Kri1tXbXUJ9SK564xLL6eQ9tSQz6QEk': './ede/ede/listValidationData.xlsx',
        '1cicAFFfrVQPfqh7j40So3bQqvrte_LtdPwTHLXh8F_A': './ede/ede/RegistroEDE.csv'
    };

  def execute(self):
    for idFile, fileName in self.filesDict.items():
      self.downloadFile(idFile,fileName);
    return True
            
  def downloadFile(self, idFile, fileName):
    urlFile = f'http://drive.google.com/uc?export=download&id={idFile}';
    urlFile2 = f'https://docs.google.com/spreadsheets/d/{idFile}/export?format=csv&id={idFile}'
    if "--debug" in sys.argv: http.client.HTTPConnection.debuglevel = 1;
    
    logger.info(f'Intentando descargar archivo: {fileName} desde url: {urlFile}');
    try:
      if (idFile == '1cicAFFfrVQPfqh7j40So3bQqvrte_LtdPwTHLXh8F_A'):
        response = requests.get(urlFile2)
      else: 
        response = requests.get(urlFile)
      #response = requests.get(urlFile);
      response.raise_for_status();
    except Exception as e:
      pathFile = None;
      logger.error(f'No se pudo descargar el arhivo {fileName}. \n {e}');
    else:
      if not os.path.exists('./ede/ede'):
        os.makedirs('./ede/ede');
      if not os.path.exists('./csv'):
        os.makedirs('./csv');

      with open(fileName,'wb') as out:
        out.write(io.BytesIO(response.content).read()) ## Read bytes into file
        pathFile = os.path.join(os.path.dirname(fileName), fileName);
        logger.info(f'Arhivo {pathFile} guardado con éxito.');
    
    return pathFile

    #def hash_bytestr_iter(bytesiter, hasher, ashexstr=False):
    #  for block in bytesiter:
    #    hasher.update(block)
    #    return hasher.hexdigest() if ashexstr else hasher.digest()

    #def file_as_blockiter(afile, blocksize=65536):
    #  with afile:
    #    block = afile.read(blocksize)
    #    while len(block) > 0:
    #      yield block
    #      block = afile.read(blocksize)

    #[(fname, hash_bytestr_iter(file_as_blockiter(open(fname, 'rb')), hashlib.sha256()))
    #    for fname in fnamelst]