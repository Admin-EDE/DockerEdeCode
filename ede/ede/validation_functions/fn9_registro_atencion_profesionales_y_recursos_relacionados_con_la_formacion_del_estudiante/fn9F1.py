from inspect import getframeinfo, currentframe
from multiprocessing import current_process

from ede.ede._logger import logger


def fn9F1(conn, return_dict):
    """
    REGISTRO DE ATENCIÓN DE PROFESIONALES Y DE RECURSOS RELACIONADOS CON LA FORMACIÓN DEL ESTUDIANTE
    6.2 Contenido mínimo, letra f
    Verificar que la planificación del proceso formativo del estudiante se encuentre registrada
    en el sistema.
    Args:
        conn ([sqlalchemy.engine.Connection]): [
          Objeto que establece la conexión con la base de datos.
          Creado previamente a través de la función execute(self)
          ]
    Returns:
        [Boolean]: [
          Retorna False y "S/Datos" a través de logger si no se encuentra información
          Retorna True y “Aprobado” a través de logger, solo si se puede: 
            - A
          En todo otro caso, retorna False y "Rechazado" a través de logger.
          ]
    """
    _r = False
    courseSections = []
    try:
        courseSections = conn.execute("""
        SELECT
          O.OrganizationId
        FROM Organization O
        WHERE
          O.RefOrganizationTypeId IN (
            SELECT RefOrganizationTypeId 
            FROM RefOrganizationType
            WHERE Code IN ('CourseSection')
          )                              
      """).fetchall()
    except Exception as e:
        logger.info(f"Resultado: {courseSections} -> {str(e)}")

    if (len(courseSections) <= 0):
        logger.info(f"S/Datos")
        return_dict[getframeinfo(currentframe()).function] = True
        logger.info(f"{current_process().name} finalizando...")
        return True

    logger.info(
        f"primer registro encontrado: {courseSections[0]} de {len(courseSections)}")
    _query = []
    try:
        _query = conn.execute("""
          SELECT
              O.OrganizationId
            , group_concat(DISTINCT CSS.ClassMeetingDays) ClassMeetingDays
            , group_concat(DISTINCT CSS.ClassBeginningTime) ClassBeginningTime
            , group_concat(DISTINCT CSS.ClassEndingTime) ClassEndingTime
            , group_concat(DISTINCT CSS.ClassPeriod) ClassPeriod
			, count(DISTINCT ocs.OrganizationCalendarSessionId) as OrganizationCalendarSessionCount
          FROM Organization O
            JOIN CourseSection CS 
              ON CS.OrganizationId = O.OrganizationId
              AND CS.RecordEndDateTime IS NULL
            JOIN CourseSectionSchedule CSS
              ON CSS.OrganizationId = O.OrganizationId
              AND CSS.RecordEndDateTime IS NULL
            JOIN OrganizationRelationship ors
              ON ors.OrganizationId = O.OrganizationId		
			  
            JOIN OrganizationCalendar orgCal
              ON orgCal.OrganizationId = O.OrganizationId
              AND CSS.RecordEndDateTime IS NULL
            JOIN OrganizationCalendarSession ocs
              ON ocs.OrganizationCalendarId = orgCal.OrganizationCalendarId		

			  
          WHERE
            O.RefOrganizationTypeId IN (
              SELECT RefOrganizationTypeId 
              FROM RefOrganizationType
              WHERE Code IN ('CourseSection')
            )
            AND ClassMeetingDays REGEXP '^[(Lunes|Martes|Miércoles|Jueves|Viernes|,)]+$'
            AND ClassPeriod REGEXP '^[(Bloque|,|\d{2})]+$'
            AND ClassBeginningTime REGEXP '^((0[0-9]|1[0-9]|2[0-3]):([0-5][0-9]):([0-5][0-9]),?){1,}$'
            AND ClassEndingTime REGEXP '^((0[0-9]|1[0-9]|2[0-3]):([0-5][0-9]):([0-5][0-9]),?){1,}$'
            AND CS.CourseId = ors.Parent_OrganizationId
            AND CS.MaximumCapacity IS NOT NULL
            AND CS.VirtualIndicator IS NOT NULL
            --AND CS.OrganizationCalendarSessionId IS NOT NULL
            AND CS.RefInstructionLanguageId IS NOT NULL
            
          GROUP BY O.OrganizationId
      """).fetchall()
    except Exception as e:
        logger.error(f"Resultado: {_query}. Mensaje: {str(e)}")
    try:
        courseSections = list([m[0]
                              for m in courseSections if m[0] is not None])

        if(len(_query) > 0):
            logger.info(
                f"primer registro encontrado: {_query[0]} de {len(_query)}")
            for row in _query:
                try:
                    OrganizationCalendarSessionCount = row[5]
                    # Verifica que tenga asociado, al menos, un organizationCalendarSession
                    if(OrganizationCalendarSessionCount > 0):
                        courseSections.remove(row[0])
                except:
                    print(f"no se pudo eliminar {row[0]}")

        if(len(courseSections) == 0):
            logger.info(f"Aprobado")
            _r = True
        else:
            logger.error(
                f"{len(courseSections)} Asignaturas tienen problemas con su planificación: {courseSections}")
            logger.error(f"Rechazado")
    except Exception as e:
        logger.error(f"No se pudo ejecutar la consulta: {str(e)}")
        logger.error(f"Rechazado")
    finally:
        return_dict[getframeinfo(currentframe()).function] = _r
        logger.info(f"{current_process().name} finalizando...")
        return _r
