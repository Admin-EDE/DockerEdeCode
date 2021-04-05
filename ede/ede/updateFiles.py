import logging
logger = logging.getLogger('root')

import os, sys
from git import Repo

class updateFiles:
  def __init__(self, args=None):
    self.args = args
    logger.info(f"typo de argumento: {type(self.args)}, valores: {self.args}");
    self.localRepoDirectory = os.path.join(os.getcwd())
    self.destination = 'master'

  def execute(self):
    if os.path.exists(self.localRepoDirectory):
      logger.info('Directorio existe, se actualizarán los archivos')
      repo = Repo(self.localRepoDirectory)
      origin = repo.remotes.origin
      origin.pull(self.destination)
    else:
      logger.info('Directorio no existe, se clonará repositorio completo desde https://github.com/Admin-EDE/DockerEdeCode')
      Repo.clone_from("https://github.com/Admin-EDE/DockerEdeCode", self.localRepoDirectory, branch=self.destination)
