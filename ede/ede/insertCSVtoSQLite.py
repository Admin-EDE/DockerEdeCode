# -*- coding: utf-8 -*-
import logging
logger = logging.getLogger('root')

import pandas as pd 
import numpy as np
from base64 import b64decode
#from base64 import b64encode
import Crypto
from Crypto.PublicKey import RSA
from Crypto.Cipher import PKCS1_OAEP
from datetime import datetime
from pytz import timezone 
import os
import requests
import io
from sqlalchemy import create_engine
import string, random
import shutil

class insert:
  def __init__(self, args):
    self.args = args
    logger.info(f"typo de argumento: {type(self.args)}, valores: {self.args}")
    #t_stamp = self.args.t_stamp;
    # self._encode = _encode
    # self._sep = _sep
    # self.t_stamp = str(int(datetime.timestamp(datetime.now(timezone('Chile/Continental')))))
    # self.path_to_dir_csv_file = path_to_dir_csv_file
    self.args.path_to_DB_file = self.cargarBaseDeDatos(f'{self.args.t_stamp}_encryptedD3.db')
    #self.secPhase = 'BD en blanco solo con parámetros definidos por Enlaces-Mineduc';
    self.args.secPhase = self.encriptarBD(self.args.path_to_DB_file)
    #logger.info(self.args.secPhase)
    self.encriptarClaveParaLaSuperintendencia()
        
  def execute(self):
    return self.transferCSVToSQL_withPandas(self.args.path_to_dir_csv_file, self.args.path_to_DB_file, self.args.secPhase)
    
  def cargarBaseDeDatos(self, fileName):
    #idFile = '1hqAjAknc6dY720X5zO_ZU2FqI_mZa3nB'
    #url_to_zipDB_file = f'http://drive.google.com/uc?export=download&id={idFile}'
    #r = requests.get(url_to_zipDB_file, stream=True)
    #with open(fileName,'wb') as out:
    #  out.write(io.BytesIO(r.content).read()) ## Read bytes into file
    path_to_DB_file = os.path.join(os.path.dirname(fileName), fileName)
    shutil.copy('./ede/ede/ceds-nds-v7_1_encryptedD3.db', path_to_DB_file)
    _t=f"Base de datos: '{path_to_DB_file}' creada exitosamente "; logger.info(_t)
    return path_to_DB_file

  def getPublicKeyFromEmail(self,email):
    try:
      url = 'https://docs.google.com/spreadsheets/d/1cicAFFfrVQPfqh7j40So3bQqvrte_LtdPwTHLXh8F_A/export?format=csv&id=1cicAFFfrVQPfqh7j40So3bQqvrte_LtdPwTHLXh8F_A'
      df = pd.read_csv(url)
    except:
      logger.info('Error al cargar planilla online, se cargará la planilla local')
      url = './ede/ede/RegistroEDE.csv'
      df = pd.read_csv(url)
    _t=f'Planilla {url} cargada satisfactoriamente'; logger.info(_t)
    _t=f"clave pública: {df[df['Dirección de correo electrónico']=='admin@ede.mineduc.cl']['Clave Pública'].values[0]}"; logger.info(_t)
    return df[df['Dirección de correo electrónico']==email]['Clave Pública'].values[0].replace('-----BEGIN PUBLIC KEY-----','').replace('-----END PUBLIC KEY-----','')
  
  # CAMBIA CLAVE A LA BD SQLCipher
  def encriptarBD(self, DB_NAME):
    secPhase = 'BD en blanco solo con parámetros definidos por Enlaces-Mineduc'
    engine = create_engine(f"sqlite+pysqlcipher://:{secPhase}@/{DB_NAME}?cipher=aes-256-cfb&kdf_iter=64000")
    conn = engine.connect()
    conn.execute(f"PRAGMA key = '{secPhase}';")
    psw = ''.join(random.choice(string.ascii_uppercase + string.ascii_lowercase + string.digits) for _ in range(50))
    conn.execute(f"PRAGMA rekey = '{psw}';")
    conn.close()
    _t = 'Cambio de clave a la BD realizado exitosamente';logger.info(_t)
    return psw

  #----------------------------------------------------------------------------
  # Encripta la clave de la BD SQLCipher utilizando la llave pública de la 
  # Superintendencia de Educación.
  #----------------------------------------------------------------------------
      #url_to_pem_file = "https://static.superintendencia-educacion.cl/KP/clave.pub.txt"
      #r = requests.get(url_to_pem_file, verify=False, stream=True)
      #path_to_public_pem_file = io.BytesIO(r.content).read()
      #publickey = RSA.importKey(path_to_public_pem_file)
  def encryptTextUsingSiePublicKey(self, txt):
    if (self.args.email):
      _t = f'email: {self.args.email}.';logger.info(_t)
      pubkey = self.getPublicKeyFromEmail(self.args.email)
    else:
      pubkey = "MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAxfLqdKTtAFwh8lPf/sjE6N3rPZqyjHNYglGQRPJ6sHHs0Ciw18v8R4eVEIwdGslFDvg3usP1PMQrW9Nyy16Sz4T5lUyPTZFgvQ0xyB1HH9gqyprxV7Rcdb5iRLj3HuUG8Bg/4mWvp5I69GpZcpPFwm0T7Y8Np1ouErf6f+Yp6c4X0JQ5Cm8EIGmom0mRw93uouYXZ+P8WMd/EEdgRl8vJpgkewt99lm5SPsW3742bgfnsT38Z2vJMziXtVIPVsdH5yKGe0arAYIY6UHC+JnOS/KjBZ609Px5Z785ZrppXiVEX0K4e294S5xhpzPuNLTAsYPfLWDjwaLZGN8hGvFSCwIDAQAB"
    keyDER = b64decode(pubkey)
    publickey = RSA.importKey(keyDER)
    encryptor = PKCS1_OAEP.new(publickey)
    encrypted = encryptor.encrypt(bytes(txt,"utf-8"))
    return encrypted

  def encriptarClaveParaLaSuperintendencia(self):
    text_file = open(f"{self.args.t_stamp}_key.txt", "w");text_file.write(self.args.secPhase);text_file.close()      
    secPhaseEncripted = self.encryptTextUsingSiePublicKey(self.args.secPhase)
    text_file = open(f"{self.args.t_stamp}_key.encrypted", "wb");text_file.write(secPhaseEncripted);text_file.close()
    _t = 'Archivo encriptado de la SIE generado exitosamente';logger.info(_t)
    return True

  def dict_factory(self, rows):
    _dTypes = {
        "bit": [pd.api.types.is_bool_dtype, pd.Int64Dtype(), "bool"],
        "integer": [pd.api.types.is_integer_dtype, pd.Int64Dtype(), "int32"],
        "varchar":  [pd.api.types.is_string_dtype, np.unicode_, "str"],
        "nvarchar":  [pd.api.types.is_string_dtype, np.unicode_, "str"],
        "char":  [pd.api.types.is_string_dtype, np.unicode_, "str"],            
        "numeric": [pd.api.types.is_float_dtype, np.float_, "float64"],
        "NUMERIC": [pd.api.types.is_float_dtype, np.float_, "float64"],      
        "datetime": [pd.api.types.is_string_dtype, np.unicode_, "str"],     
        "TEXT": [pd.api.types.is_string_dtype, np.unicode_, "str"],     
        }    
  
    d = {}
    for col in rows:
      logger.info(col)
      try:
        d[col[1]] = _dTypes[''.join([s for s in list(col[2]) if s.isalpha()])][1]
      except:
        logger.info(f"ERROR: Tipo de datos {col[2]} no encontrado, se procesará como 'np.unicode_' ...")
        d[col[1]] = np.unicode_
    return d

  def transferCSVToSQL_withPandas(self, path_to_dir_csv_file, DB_NAME, secPhase):
    _r = True
    engine = create_engine(f"sqlite+pysqlcipher://:{secPhase}@/{DB_NAME}?cipher=aes-256-cfb&kdf_iter=64000")
    conn = engine.connect()
    for root, dirs, files in os.walk(path_to_dir_csv_file, topdown=False):
      for name in files:
        logger.info(f"\nProcesando archivo {name}...")
        tbl = name[:-4]
        rows = conn.execute(f"pragma table_info('{tbl}');")
        logger.info(f"Abriendo tabla: '{tbl}' y obteniendo sus columnas... {rows.returns_rows}")
        if(not rows.returns_rows):
          logger.info(f"No existe tabla con el nombre: '{tbl}', no se procesará el archivo...")
          continue

        jsonCol = self.dict_factory(rows)
        logger.info(f"jsonCol: {jsonCol}")

        _fileName = os.path.join(root, name)
        df = pd.read_csv(_fileName,sep=self.args._sep,encoding=self.args._encode,dtype=jsonCol)
        _c = str(df.count()[0])                
        logger.info(f'Leyendo: {_fileName} -> {_c} registros procesados.')

        try:
          df.to_sql(tbl, con = conn, index=False, if_exists='append')
          _result = "OK"
        except Exception as e:
          logger.info(f'RollBack')
          _result='ERROR: '+str(e)
          _r = False
          pass
        finally:
          logger.info(f"name: {name}")
          #self.dfLog.loc[self.dfLog['csv']==_fileName,self.dfLog.columns=='#readingRows'] = _c
          #self.dfLog.loc[self.dfLog['csv']==_fileName,self.dfLog.columns=='resultReading'] = _result                              
          logger.info(f"Table: {tbl} #Rows: {len(df.index)} {_result}")
    conn.close()
    return _r