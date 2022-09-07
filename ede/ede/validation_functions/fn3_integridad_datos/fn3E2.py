from inspect import getframeinfo, currentframe
from multiprocessing import current_process

from ede.ede.validation_functions.check_bd_utils import ejecutar_sql
from ede.ede._logger import logger


def fn3E2(conn, return_dict):
    """
    INTEGRIDAD DE DATOS
    
    Verifica que los establecimientos tengan su RBD (identificador)
    --------------------------------------------------
    VERIFICA SI LA TABLA k12schoolList unida a organizationList contiene información
    Args:
        conn ([sqlalchemy.engine.Connection]): [
          Objeto que establece la conexión con la base de datos.
          Creado previamente a través de la función execute(self)
          ]
    Returns:
        [Boolean]: [
          Retorna True/False y "S/Datos" a través de logger, solo si puede:
            - No hay establecimientos en la base de datos
          Retorna True y “Aprobado” a través de logger, solo si se puede: 
            - Los establecimientos tienen su identificador RBD
          En todo otro caso, retorna False y "Rechazado" a través de logger.
          ]
    """
    _r = False
    rows = []
    try:
        rows = ejecutar_sql(conn, """--sql
        SELECT Identifier 
        FROM k12schoolList 
          INNER JOIN organizationList 
            USING(OrganizationId);""")
    except Exception as e:
        logger.info(f"Resultado: {rows} -> {str(e)}")

    try:
        logger.info(f"len(establecimientos): {len(rows)}")
        if(len(rows) > 0):
            logger.info(f"Aprobado")
            _r = True
        else:
            logger.info(f"S/Datos")
            _r = True

    except Exception as e:
        logger.error(
            f"NO se pudo ejecutar la consulta a la tabla k12schoolList para identificar el RBD del establecimiento: {str(e)}")
        logger.error(f"Rechazado")
        _r = False
    finally:
        return_dict[getframeinfo(currentframe()).function] = _r
        logger.info(f"{current_process().name} finalizando...")
        return _r
