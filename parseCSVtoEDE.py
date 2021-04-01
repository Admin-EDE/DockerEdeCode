# -*- coding: utf-8 -*-
import requests, io, os, sys

def downloadFile(idFile='1j-1QXj-vM-pTYxsemzjFaszs-Sgwf4iM',fileName='ede.py'):
  urlFile = f'http://drive.google.com/uc?export=download&id={idFile}' 
  print(f"Intentando descargar el archivo '{fileName}' desde '{urlFile}'...")
  try:
    response = requests.get(urlFile, stream=True)
    response.raise_for_status()
  except:
    pathFile = None
  else:
    with open(fileName,'wb') as out:
      out.write(io.BytesIO(response.content).read()) ## Read bytes into file
    pathFile = os.path.join(os.path.dirname(fileName), fileName)
  return pathFile

def main():
  print(f"Iniciando programa...")
  fileName='ede.py'
  #pathDownloadFile = downloadFile()
  if(len(sys.argv) < 2):
    sys.argv.append('--help')

  if('update' in sys.argv):
    pathDownloadFile = downloadFile()
  else:
    pathDownloadFile = None

  if (not pathDownloadFile):
    print(f"No se descargó el archivo '{fileName}' desde Internet...\nSe buscará una copia local...")
    if (os.path.exists(fileName)):
      print(f"Se encontró una copia local del archivo '{fileName}'...")
      pathDownloadFile = fileName

  if(pathDownloadFile):
    print(f"Llamando el archivo: {pathDownloadFile}")
    import ede
    ede.main()
  else:
    print(f"No se pudo descargar el archivo '{fileName}' y tampoco se encontró una copia local del archivo.")
    print("Si lo desea puede utilizar el argumento --debug para obtener más detalles del error.\n")
    print("Si no tiene acceso a Internet, descargue una copia manualmente del archivo en su carpeta local.")

if __name__== "__main__":
  main()