from inspect import getframeinfo, currentframe
from multiprocessing import current_process

from ede.ede.validation_functions.check_bd_utils import ejecutar_sql
from ede.ede._logger import logger


def fn8F1(conn, return_dict):
    """
    REGISTRO DE ANOTACIONES DE CONVIVENCIA ESCOLAR POR ESTUDIANTE
    6.2 Contenido mínimo, letra e
    Verificar la aplicación y seguimiento de medidas disciplinarias relacionadas con el reglamento interno.
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
    try:
        query = ejecutar_sql(conn, """--sql
SELECT
	 Inc.incidentId
FROM Incident Inc
JOIN RefIncidentBehavior rInBh
	ON rInBh.RefIncidentBehaviorId = Inc.RefIncidentBehaviorId
	AND rInBh.description not in (
               'Entrevista'
              ,'Reunión con apoderados'
              ,'Entrega de documentos retiro de un estudiante'
              ,'Anotación positiva'
              ,'Entrega de documentos de interés general'
              ,'Entrega de información para continuidad de estudios')
                             """)
        if(len(query) > 0):
            Incidentes = (list([m[0] for m in query if m[0] is not None]))
            for x in Incidentes:
                querySelect = "SELECT * from K12StudentDiscipline where IncidentId = "
                queryWhere = str(x)
                queryComplete = querySelect+queryWhere
                try:
                    query = ejecutar_sql(conn, queryComplete)
                    if(len(query) > 0):
                        query = len(query)
                        logger.info(f'Total de datos: {query}')
                        logger.info(f'Aprobado')
                        return_dict[getframeinfo(
                            currentframe()).function] = True
                        logger.info(f"{current_process().name} finalizando...")
                        return True
                    else:
                        logger.error(f'S/Datos')
                        logger.error(
                            f'No se encuentran registradas medidas diciplinarias para los incidentes registrados')
                        return_dict[getframeinfo(
                            currentframe()).function] = False
                        logger.info(f"{current_process().name} finalizando...")
                        return False
                except Exception as e:
                    logger.error(f'No se pudo ejecutar la consulta: {str(e)}')
                    logger.error(f'Rechazado')
                    _r = False
                    return_dict[getframeinfo(currentframe()).function] = False
                    return False
        else:
            logger.info(f'S/Datos')
            logger.info(f'Sin incidentes registrados')
            return_dict[getframeinfo(currentframe()).function] = True
            logger.info(f"{current_process().name} finalizando...")
            return True
    except Exception as e:
        logger.error(f"No se pudo ejecutar la consulta: {str(e)}")
        logger.error(f"Rechazado")
        return_dict[getframeinfo(currentframe()).function] = False
        logger.info(f"{current_process().name} finalizando...")
        return False
