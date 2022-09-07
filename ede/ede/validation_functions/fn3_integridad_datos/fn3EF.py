from inspect import getframeinfo, currentframe
from multiprocessing import current_process

from ede.ede.validation_functions.check_bd_utils import ejecutar_sql
from ede.ede._logger import logger


def fn3EF(conn, return_dict):
    """
    INTEGRIDAD DE DATOS
    
    Verifica que los id del curso sean equivalentes en las tablas course y organization
    Args:
        conn ([sqlalchemy.engine.Connection]): [
          Objeto que establece la conexión con la base de datos.
          Creado previamente a través de la función execute(self)
          ]
    Returns:
        [Boolean]: [
          Retorna True/False y "S/Datos" a través de logger, solo si puede:
            - No hay datos de cursos en las tablas course u organization
          Retorna True y “Aprobado” a través de logger, solo si se puede: 
            - Los id de los cursos son equivalentes en las tablas course y organization
          En todo otro caso, retorna False y "Rechazado" a través de logger.
          ]
    """
    _r = False
    courses1 = []
    try:
        courses1 = ejecutar_sql(conn, 
            "SELECT DISTINCT OrganizationIdDelCurso FROM jerarquiasList;")
    except Exception as e:
        logger.info(f"Resultado: {courses1} -> {str(e)}")

    courses2 = []
    try:
        courses2 = ejecutar_sql(conn, 
            "SELECT OrganizationIdCurso FROM cursoList;")
    except Exception as e:
        logger.info(f"Resultado: {courses2} -> {str(e)}")

    try:
        if(len(courses1) > 0 and len(courses2) > 0):
            # Valida que lista de cursos coincidan
            curso1 = list(set([m[0] for m in courses1 if m[0] is not None]))
            curso2 = list(set([m[0] for m in courses2 if m[0] is not None]))
            _c = len(set(curso1) & set(curso2))
            _err = "No coinciden los ID de Curso en las tablas Organization + Course + K12Course"
            if _c == len(curso1) == len(curso2):
                logger.info(f"Aprobado")
                _r = True
            else:
                logger.error(_err)
        else:
            logger.info(f"S/Datos")
            _r = True
    except Exception as e:
        logger.error(
            f"NO se pudo ejecutar la consulta a la vista jerarquiasList para obtener la lista de organizaciones: {str(e)}")
        logger.error(f"Rechazado")
        _r = False
    finally:
        return_dict[getframeinfo(currentframe()).function] = _r
        logger.info(f"{current_process().name} finalizando...")
        return _r
