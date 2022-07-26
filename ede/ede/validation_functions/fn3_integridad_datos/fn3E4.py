from inspect import getframeinfo, currentframe
from multiprocessing import current_process

import ede.ede.check_utils as check_utils
from ede.ede._logger import logger


# VERIFICA SI LA VISTA jerarquiasList contiene información
def fn3E4(conn, return_dict):
    """
    Verifica que los cursos tengan una organización, rbd, nivel, jornada, etc
    Args:
        conn ([sqlalchemy.engine.Connection]): [
          Objeto que establece la conexión con la base de datos.
          Creado previamente a través de la función execute(self)
          ]
    Returns:
        [Boolean]: [
          Retorna True/False y "S/Datos" a través de logger, solo si puede:
            - No hay datos para la vista jerarquiasList
          Retorna True y “Aprobado” a través de logger, solo si se puede: 
            - Hay cursos
          En todo otro caso, retorna False y "Rechazado" a través de logger.
          ]
    """
    _r = False
    rows = []
    try:
        rows = conn.execute(
            "SELECT RBD,nombreEstablecimiento,modalidad,jornada,nivel,rama,sector,especialidad,tipoCurso,codigoEnseñanza,grado,letraCurso FROM jerarquiasList;").fetchall()
    except Exception as e:
        logger.info(f"Resultado: {rows} -> {str(e)}")
    try:
        logger.info(f"len(organizaciones): {len(rows)}")
        if(len(rows) > 0):
            logger.info(f"Aprobado")
            _r = True
        else:
            logger.info(f"S/Datos")
    except Exception as e:
        logger.error(
            f"NO se pudo ejecutar la consulta a la vista jerarquiasList para obtener la lista de organizaciones: {str(e)}")
        logger.error(f"Rechazado")
    finally:
        return_dict[getframeinfo(currentframe()).function] = _r
        logger.info(f"{current_process().name} finalizando...")
        return _r
