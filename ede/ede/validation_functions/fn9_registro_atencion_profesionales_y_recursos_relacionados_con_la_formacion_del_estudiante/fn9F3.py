from inspect import getframeinfo, currentframe
from multiprocessing import current_process

from ede.ede.validation_functions.check_bd_utils import ejecutar_sql
from ede.ede._logger import logger


def fn9F3(conn, return_dict):
    """
    REGISTRO DE ATENCIÓN DE PROFESIONALES Y DE RECURSOS RELACIONADOS CON LA FORMACIÓN DEL ESTUDIANTE
    6.2 Contenido mínimo, letra f
    Verificar que el registro de actividades con la familia 
    y la comunidad se encuentre cargado en el sistema.
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
        incident = ejecutar_sql(conn, """--sql
          SELECT IncidentId
          from Incident
          WHERE
            RefIncidentBehaviorId IN (
              SELECT RefIncidentBehaviorId
              FROM RefIncidentBehavior
              WHERE RefIncidentBehavior.description IN ('Reunión con apoderados','Entrevista')
            );
        """)
        if (len(incident) > 0):
            listIncident = (list([m[0] for m in incident if m[0] is not None]))
            for x in listIncident:
                try:
                    x = str(x)
                    incidentParent = ejecutar_sql(conn, """--sql
                      SELECT IncidentId 
                      FROM IncidentPerson 
                      where 
                      IncidentId = """+x+"""--sql
                      and 
                      (
                        IncidentPerson.RefIncidentPersonRoleTypeId IN (
                          SELECT RefIncidentPersonRoleTypeId
                          FROM RefIncidentPersonRoleType
                          WHERE RefIncidentPersonRoleType.description IN ('Asiste a reunión de apoderados')
                        )
                        and 
                        IncidentPerson.RefIncidentPersonTypeId IN (
                          SELECT RefIncidentPersonTypeId
                          FROM RefIncidentPersonType
                          WHERE RefIncidentPersonType.description IN ('Apoderado')
                        )
                        OR
                        IncidentPerson.RefIncidentPersonRoleTypeId IN (
                          SELECT RefIncidentPersonRoleTypeId
                          FROM RefIncidentPersonRoleType
                          WHERE RefIncidentPersonRoleType.description IN ('Entrevistado')
                        )
                        and 
                        IncidentPerson.RefIncidentPersonTypeId IN (
                          SELECT RefIncidentPersonTypeId
                          FROM RefIncidentPersonType
                          WHERE RefIncidentPersonType.description IN ('Apoderado')
                        )	
                      )                                                      
                    """)
                    incidentProfessor = ejecutar_sql(conn, """--sql
                        SELECT IncidentId 
                        FROM IncidentPerson 
                        where 
                        IncidentId = """+x+"""--sql
                        and 
                        (
                          IncidentPerson.RefIncidentPersonRoleTypeId IN (
                            SELECT RefIncidentPersonRoleTypeId
                            FROM RefIncidentPersonRoleType
                            WHERE RefIncidentPersonRoleType.description IN ('Dirige reunión de apoderados')
                          )
                          and 
                          IncidentPerson.RefIncidentPersonTypeId IN (
                            SELECT RefIncidentPersonTypeId
                            FROM RefIncidentPersonType
                            WHERE RefIncidentPersonType.description IN ('Docente','Profesional de la educación','Personal Administrativo')
                          )
                          OR
                          IncidentPerson.RefIncidentPersonRoleTypeId IN (
                            SELECT RefIncidentPersonRoleTypeId
                            FROM RefIncidentPersonRoleType
                            WHERE RefIncidentPersonRoleType.description IN ('Entrevistador')
                          )
                          and 
                          IncidentPerson.RefIncidentPersonTypeId IN (
                            SELECT RefIncidentPersonTypeId
                            FROM RefIncidentPersonType
                            WHERE RefIncidentPersonType.description IN ('Docente','Profesional de la educación','Personal Administrativo')
                          )	
                        )                                                         
                    """)
                    parent = 0
                    professor = 0
                    if (len(incidentParent) > 0):
                        parent += 1
                    else:
                        logger.info(f"S/Datos")
                        logger.info(
                            f"Sin registros de actividades familiares o comunitarias")
                        return_dict[getframeinfo(
                            currentframe()).function] = True
                        logger.info(f"{current_process().name} finalizando...")
                        return True
                    if (len(incidentProfessor) > 0):
                        professor += 1
                    else:
                        logger.info(f"S/Datos")
                        logger.info(
                            f"Sin registros de actividades familiares o comunitarias")
                        return_dict[getframeinfo(
                            currentframe()).function] = True
                        logger.info(f"{current_process().name} finalizando...")
                        return True
                except Exception as e:
                    logger.error(f"No se pudo ejecutar la consulta: {str(e)}")
                    logger.error(f"Rechazado")
                    return_dict[getframeinfo(currentframe()).function] = False
                    logger.info(f"{current_process().name} finalizando...")
                    return False
            logger.info(f'Reuniones validas')
            logger.info(f'Aprobado')
            return_dict[getframeinfo(currentframe()).function] = True
            logger.info(f"{current_process().name} finalizando...")
            return True
        else:
            logger.info(f"S/Datos")
            logger.info(
                f"Sin registros de actividades familiares o comunitarias")
            return_dict[getframeinfo(currentframe()).function] = True
            logger.info(f"{current_process().name} finalizando...")
            return True
    except Exception as e:
        logger.error(f"No se pudo ejecutar la consulta: {str(e)}")
        logger.error(f"Rechazado")
        return_dict[getframeinfo(currentframe()).function] = False
        logger.info(f"{current_process().name} finalizando...")
        return False
