from inspect import getframeinfo, currentframe
from multiprocessing import current_process

from ede.ede.validation_functions.check_bd_utils import ejecutar_sql
from ede.ede._logger import logger


def fn3D9(conn, return_dict):
    """
    INTEGRIDAD DE DATOS

    Las asignaturas tienen sus sesiones de calendario (cuándo será la clase) y sus asistencias.
    -----
    (Tabla RoleAttendance)
    Args:
        conn ([sqlalchemy.engine.Connection]): [
          Objeto que establece la conexión con la base de datos.
          Creado previamente a través de la función execute(self)
          ]
    Returns:
        [Boolean]: [
          Retorna True/False y "S/Datos" a través de logger, solo si puede:
            - No existe registro de calendar session o role attendance event
          Retorna True y “Aprobado” a través de logger, solo si se puede: 
            - Existen registros de calendar session y attendance por asignatura
          En todo otro caso, retorna False y "Rechazado" a través de logger.
          ]
    """
    _r = False
    listInfoSuccesfull = []
    try:
        listInfoSuccesfull = ejecutar_sql(conn, """--sql
        /*
        * verifica que existan registro de calendar Session y RoleAttendanceEvent.
        */
        SELECT Organization.OrganizationId, RoleAttendanceEvent.RoleAttendanceEventid, OrganizationCalendarSession.OrganizationCalendarSessionId
        FROM Organization
        INNER JOIN RefOrganizationType
         ON Organization.RefOrganizationTypeId = RefOrganizationType.RefOrganizationTypeId
         AND RefOrganizationType.RefOrganizationTypeId == 22 -- 'Course Section'
        INNER JOIN OrganizationCalendar
         ON Organization.OrganizationId = OrganizationCalendar.OrganizationId
        INNER JOIN OrganizationCalendarSession
         ON OrganizationCalendar.OrganizationCalendarId = OrganizationCalendarSession.OrganizationCalendarId
         AND OrganizationCalendarSession.AttendanceTermIndicator = 1
        INNER JOIN OrganizationPersonRole
         ON Organization.OrganizationId = OrganizationPersonRole.OrganizationId
        INNER JOIN RoleAttendanceEvent
         ON OrganizationPersonRole.OrganizationPersonRoleId = RoleAttendanceEvent.OrganizationPersonRoleId
		 WHERE
		   Organization.RefOrganizationTypeId = 22 --.Description = 'Course Section'
		   AND OrganizationCalendarSession.AttendanceTermIndicator = 1
       AND DATE(RoleAttendanceEvent.Date) = OrganizationCalendarSession.BeginDate
		   AND DATE(RoleAttendanceEvent.Date) = OrganizationCalendarSession.EndDate
      """)
    except Exception as e:
        logger.error(f'Rechazado')
        _r = False
        logger.info(f"Resultado: {listInfoSuccesfull} -> {str(e)}")
        return_dict[getframeinfo(currentframe()).function] = _r
        return _r

    if(len(listInfoSuccesfull) <= 0):
        logger.info("S/Datos")
        _r = True
        return_dict[getframeinfo(currentframe()).function] = _r
        logger.info(f"{current_process().name} finalizando...")
        return _r

    RoleAttendance = []
    try:
        RoleAttendance_outer = ejecutar_sql(conn, """--sql
        /*
        * verifica que los registro de calendar Session y RoleAttendanceEvent sean consistentes.
        */
        SELECT Organization.OrganizationId, RoleAttendanceEvent.RoleAttendanceEventid, OrganizationCalendarSession.OrganizationCalendarSessionId
        FROM Organization
        OUTER LEFT JOIN RefOrganizationType
         ON Organization.RefOrganizationTypeId = RefOrganizationType.RefOrganizationTypeId
         AND RefOrganizationType.RefOrganizationTypeId == 22 -- 'Course Section'
        OUTER LEFT JOIN OrganizationCalendar
         ON Organization.OrganizationId = OrganizationCalendar.OrganizationId
        OUTER LEFT JOIN OrganizationCalendarSession
         ON OrganizationCalendar.OrganizationCalendarId = OrganizationCalendarSession.OrganizationCalendarId
         AND OrganizationCalendarSession.AttendanceTermIndicator = 1
        OUTER LEFT JOIN OrganizationPersonRole
         ON Organization.OrganizationId = OrganizationPersonRole.OrganizationId
        OUTER LEFT JOIN RoleAttendanceEvent
         ON OrganizationPersonRole.OrganizationPersonRoleId = RoleAttendanceEvent.OrganizationPersonRoleId
		 WHERE
		   Organization.RefOrganizationTypeId = 22 --.Description = 'Course Section'
		   AND OrganizationCalendarSession.AttendanceTermIndicator = 1
       AND DATE(RoleAttendanceEvent.Date) = OrganizationCalendarSession.BeginDate
		   AND DATE(RoleAttendanceEvent.Date) = OrganizationCalendarSession.EndDate
      """)
    except Exception as e:
        logger.error(f"Rechazado")
        logger.info(f"Resultado: {RoleAttendance_outer} -> {str(e)}")

    logger.info(
        f"Eventos mal identificados: {len(RoleAttendance_outer) - len(listInfoSuccesfull)}")
    try:
        if(len(RoleAttendance_outer) - len(listInfoSuccesfull) > 0):
            for x in RoleAttendance_outer:
                if x not in listInfoSuccesfull:
                    RoleAttendance.append(x)
                data1 = list(
                    set([m[0] for m in RoleAttendance if m[0] is not None]))
                _c1 = len(set(data1))
                _err1 = f"Las siguientes organizaciones no coinciden: {data1}"
                if (_c1 > 0):
                    logger.error(_err1)
                    logger.error(f"Rechazado")
                    _r = False
                else:
                    logger.info(f"Aprobado")
                    _r = True
        else:
            logger.info(f"Aprobado")
            _r = True
    except Exception as e:
        logger.error(
            f"NO se pudo ejecutar la consulta a la verificación: {str(e)}")
        logger.error(f"Rechazado")
    finally:
        logger.info(f'Aprobado') if _r else logger.error(f'Rechazado')
        return_dict[getframeinfo(currentframe()).function] = _r
        logger.info(f"{current_process().name} finalizando...")
        return _r
