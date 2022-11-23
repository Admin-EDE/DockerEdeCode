from inspect import getframeinfo, currentframe
from multiprocessing import current_process
import pandas as pd

from ede.ede.validation_functions.check_bd_utils import ejecutar_sql
from ede.ede._logger import logger


def fn3F1(conn, return_dict, selfargs):
    """
    INTEGRIDAD DE DATOS
    
    La integridad referencial de los datos es correcta.
    Args:
        conn ([sqlalchemy.engine.Connection]): [
          Objeto que establece la conexión con la base de datos.
          Creado previamente a través de la función execute(self)
          ]
    Returns:
        [Boolean]: [
          Regresa True y "Aprobado" a través de logger, ssi puede:
            - No contiene errores de integridad referencial en la BD.
          En todo otro caso:
            - Agrega un archivo “_ForenKeyErrors.csv” al “_Data.ZIP” que contiene el resultado final de la revisión y
            - Regresa False y “Rechazado” a través de logger.
          ]
    """
    _r = False
    rows = []
    try:
        rows = ejecutar_sql(conn, "PRAGMA foreign_key_check;")
    except Exception as e:
        logger.info(f"Resultado: {rows} -> {str(e)}")
    try:
        if(len(rows) > 0):
            pd.DataFrame(rows, columns=['Table', 'rowId', 'Parent', 'FkId']).to_csv(
                selfargs._FKErrorsFile, sep=selfargs._sep, encoding=selfargs._encode, index=False)
            logger.error(
                f"BD con errores de integridad referencial, más detallen en {selfargs._FKErrorsFile}")
        else:
            _r = True
        logger.info("Aprobado") if _r else logger.error("Rechazado")
    except Exception as e:
        logger.error(f"Error al ejecutar la función: {str(e)}")
    finally:
        return_dict[getframeinfo(currentframe()).function] = _r
        logger.info(f"{current_process().name} finalizando...")
        return _r
