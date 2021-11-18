import logging
import requests, io
logger = logging.getLogger('root')

import os, sys
from git import Repo

class updateFiles:
  def __init__(self, args=None):
    self.args = args
    logger.info(f"typo de argumento: {type(self.args)}, valores: {self.args}")
    self.filesDict = {
        '1cicAFFfrVQPfqh7j40So3bQqvrte_LtdPwTHLXh8F_A': './ede/ede/RegistroEDE.csv'
    }    
    self.localRepoDirectory = os.path.join(os.getcwd())
    self.destination = 'master'

  def execute(self):
    if os.path.exists(self.localRepoDirectory):
      logger.info('Directorio existe, se actualizarán los archivos')
      try:
        repo = Repo(self.localRepoDirectory)
      except:
        logger.error(f'Por favor verifique que exista el directorio ".git" en {self.localRepoDirectory}')
        return False
      origin = repo.remotes.origin
      origin.pull(self.destination)
    else:
      logger.info('Directorio no existe, se clonará repositorio completo desde https://github.com/Admin-EDE/DockerEdeCode')
      Repo.clone_from("https://github.com/Admin-EDE/DockerEdeCode", self.localRepoDirectory, branch=self.destination)

    for idFile, fileName in self.filesDict.items():
          self.downloadFile(idFile,fileName)

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
        pathFile = fileName
        logger.info(f'Arhivo {pathFile} guardado con éxito.')
    
    return pathFile
