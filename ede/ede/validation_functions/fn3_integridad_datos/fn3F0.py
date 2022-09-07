from inspect import getframeinfo, currentframe
from multiprocessing import current_process

from ede.ede.validation_functions.check_bd_utils import ejecutar_sql
from ede.ede._logger import logger


def fn3F0(conn, return_dict):
    """
    INTEGRIDAD DE DATOS
    
    Verifica la conexión con la base de datos SQLCypher
    Args:
        conn ([sqlalchemy.engine.Connection]): [
          Objeto que establece la conexión con la base de datos.
          Creado previamente a través de la función execute(self)
          ]
    Returns:
        [Boolean]: [
          Retorna True y “Aprobado” a través de logger, solo si se puede: 
            - desencriptar la base de datos, 
            - obtener su clave secreta, 
            - establecer la conexión y 
            - obtener al menos un registro de la vista personList. 
          En todo otro caso, retorna False y "Rechazado" a través de logger.
          ]
    """
    _r = False
    rows = []
    try:
        rows = ejecutar_sql(conn, "SELECT personId FROM PersonList;")
    except Exception as e:
        logger.error(f"Error al ejecutar la función: {str(e)}")

    try:
        if(len(rows) > 0):
            _r = True
        logger.info("Aprobado") if _r else logger.error("Rechazado")
    except Exception as e:
        logger.error(f"Error al ejecutar la función: {str(e)}")
        logger.error("Rechazado")
    finally:
        return_dict[getframeinfo(currentframe()).function] = _r
        logger.info(f"{current_process().name} finalizando...")
        return _r
