from inspect import getframeinfo, currentframe
from multiprocessing import current_process

import ede.ede.check_utils as check_utils
from ede.ede._logger import logger

# VERIFICA SI LA TABLA k12schoolList unida a organizationList contiene información


def fn3E2(conn, return_dict):
    """ Breve descripción de la función
    Args:
        conn ([sqlalchemy.engine.Connection]): [
          Objeto que establece la conexión con la base de datos.
          Creado previamente a través de la función execute(self)
          ]
    Returns:
        [Boolean]: [
          Retorna True/False y "S/Datos" a través de logger, solo si puede:
            - A
          Retorna True y “Aprobado” a través de logger, solo si se puede: 
            - A
          En todo otro caso, retorna False y "Rechazado" a través de logger.
          ]
    """
    _r = False
    rows = []
    try:
        rows = conn.execute("""
        SELECT Identifier 
        FROM k12schoolList 
          INNER JOIN organizationList 
            USING(OrganizationId);""").fetchall()
    except Exception as e:
        logger.info(f"Resultado: {rows} -> {str(e)}")

    try:
        logger.info(f"len(establecimientos): {len(rows)}")
        if(len(rows) > 0):
            logger.info(f"Aprobado")
            _r = True
        else:
            logger.info(f"S/Datos")

    except Exception as e:
        logger.error(
            f"NO se pudo ejecutar la consulta a la tabla k12schoolList para identificar el RBD del establecimiento: {str(e)}")
        logger.error(f"Rechazado")
    finally:
        return_dict[getframeinfo(currentframe()).function] = _r
        logger.info(f"{current_process().name} finalizando...")
        return _r
