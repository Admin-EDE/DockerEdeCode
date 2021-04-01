import logging
logger = logging.getLogger('root')

import os, sys

class updateFiles:
  def __init__(self, args=None):
    self.args = args
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
    }

  def execute(self):
    for idFile, fileName in self.filesDict.items():
      self.downloadFile(idFile,fileName)
    return True
            
  def downloadFile(self, idFile, fileName):
    urlFile = f'http://drive.google.com/uc?export=download&id={idFile}'
    urlFile2 = f'https://docs.google.com/spreadsheets/d/{idFile}/export?format=csv&id={idFile}'
    if "--debug" in sys.argv: http.client.HTTPConnection.debuglevel = 1
    
    logger.info(f'Intentando descargar archivo: {fileName} desde url: {urlFile}')
    try:
      if (idFile == '1cicAFFfrVQPfqh7j40So3bQqvrte_LtdPwTHLXh8F_A'):
        response = requests.get(urlFile2)
      else: 
        response = requests.get(urlFile)
      #response = requests.get(urlFile);
      response.raise_for_status()
    except Exception as e:
      pathFile = None
      logger.error(f'No se pudo descargar el arhivo {fileName}. \n {e}')
    else:
      if not os.path.exists('./ede/ede'):
        os.makedirs('./ede/ede')
      if not os.path.exists('./csv'):
        os.makedirs('./csv')

      with open(fileName,'wb') as out:
        out.write(io.BytesIO(response.content).read()) ## Read bytes into file
        pathFile = os.path.join(os.path.dirname(fileName), fileName)
        logger.info(f'Arhivo {pathFile} guardado con éxito.')
    
    return pathFile