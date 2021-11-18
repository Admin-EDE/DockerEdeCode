import argparse #Librería usada en funcion main() para crear el menú de la consola
import importlib.util #utilizada en función module_from_file para cargar las librerías del estándar
import logging
logger = logging.getLogger('root')

class consoleMenu:
  def __init__(self):
    self.parser = argparse.ArgumentParser()
    subparsers = self.parser.add_subparsers(help='Comandos permitdos en el  sistema ')

    # create the parser for the "update" command
    parser_update = subparsers.add_parser('update', help='Actualiza los archivos del sistema')
    parser_update.set_defaults(func=self.update)

    # create the parser for the "parse" command
    parser_parseCSVtoSQL = subparsers.add_parser('parse', help='Transforma el archivo de entrada en varios CSV. Uno por cada tabla del modelo')
    parser_parseCSVtoSQL.add_argument('source', choices=['json', 'xlsx'], help="tipo de archivo a transformar")
    parser_parseCSVtoSQL.add_argument('-nz','--nozip', help='Indica que el archivo no esta comprimido', action="store_true")
    parser_parseCSVtoSQL.add_argument('path_to_file', type=str, help="nombre del archivo a transformar")
    parser_parseCSVtoSQL.add_argument('-d','--debug', help='Aumenta el detalle de la salida', action="store_true")
    parser_parseCSVtoSQL.add_argument('-o', '--output', type=str, help='Indica el nombre del archivo de salida')
    parser_parseCSVtoSQL.set_defaults(func=self.parse)
    
    # create the parser for the "insert" command
    parser_insert = subparsers.add_parser('insert', help='Inserta todos los archivos de la carpeta csv a una BD SQLite encriptada y reporta la validez de los datos insertados')
    parser_insert.add_argument('--NoValidate', action='store_true', help="Desabilita la validación por defecto de los datos")
    parser_insert.add_argument('-d','--debug', help='Aumenta el detalle de la salida', action="store_true")
    parser_insert.add_argument('-e','--email', type=str, help='Indica el email del destinatario')    
    parser_insert.add_argument('-o','--output', type=str, help='Indica el nombre del archivo de salida')
    parser_insert.set_defaults(func=self.insert)
    
    # create the parser for the "check" command
    parser_check = subparsers.add_parser('check', help='Chequea la información almacena en la BD y entrega un reporte')
    parser_check.add_argument('secPhase', type=str, help="Corresponde a la contraseña de la BD")
    parser_check.add_argument('path_to_DB_file', type=str, help="Nombre del archivo que contiene la BD SQLite encriptada")
    parser_check.add_argument('-d','--debug', help='Aumenta el detalle de la salida', action="store_true")
    parser_check.add_argument('-j','--json', help='El archivo de resultados en formato JSON', action="store_true")
    parser_check.add_argument('-f','--function', type=str, help='Especifica la función que se desea revisar')    
    parser_check.set_defaults(func=self.check)
    
  def module_from_file(self,module_name, file_path):
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module
    
  #----------------------------------------------------------------------------
  # Clean or Create CSV directory
  #----------------------------------------------------------------------------
  def cleanDirectory(self, d):
    if(not os.path.exists(d)):
        os.mkdir(d)
    else:
        for root, dirs, files in os.walk(d, topdown=False):
            for name in files:
                os.remove(os.path.join(root, name))
            for name in dirs:
                os.rmdir(os.path.join(root, name))
    _t = f'Directorio : {d} limpio y preparado con éxito!!!' ;logger.info(_t)
    return True

  def update(self, args):
    logger.info(f"Iniciando update: {args}")
    module = self.module_from_file("ede", "./ede/ede/updateFiles.py")
    updateFile = module.updateFiles(args)
    updateFile.execute()

  def parse(self, args):
    logger.info(f"Iniciando parse: {args}")
    if(args.source == 'json'):   module = self.module_from_file("ede", "./ede/ede/parseJSONtoCSV.py")
    elif(args.source == 'xlsx'): module = self.module_from_file("ede", "./ede/ede/parseXLSXtoCSV.py")
    parseFile = module.parse(args)
    parseFile.execute()

  def insert(self, args):
    logger.info(f"Iniciando insert: {args}")
    module = self.module_from_file("ede", "./ede/ede/insertCSVtoSQLite.py")
    insertCSVtoSQLite = module.insert(args)
    insertCSVtoSQLite.execute()

    if(not args.NoValidate):
      args.secPhase = insertCSVtoSQLite.args.secPhase
      args.path_to_DB_file = insertCSVtoSQLite.args.path_to_DB_file
      self.check(args)
    else:
      logging.warning("No se realiza validación de los datos, el parámetro '--NoValidate' se encuentra activado...")
    
  def check(self, args):
    logger.info(f"Iniciando check: args")
    module = self.module_from_file("ede", "./ede/ede/checkSQLiteEDE.py")
    checkSQLiteEDE = module.check(args)
    checkSQLiteEDE.execute()