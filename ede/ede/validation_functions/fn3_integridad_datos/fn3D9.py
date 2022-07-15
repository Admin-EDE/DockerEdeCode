from inspect import getframeinfo, currentframe
from multiprocessing import current_process


from ede.ede._logger import logger


  # Revisar que los cursos del establecimiento tengan bien 
  # calculada la información de la tabla RoleAttendance.
def fn3D9(conn, return_dict):
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
    listInfoSuccesfull = []
    try:
      listInfoSuccesfull = conn.execute("""
        /*
        * verifica que los registro de calendar Session y RoleAttendanceEvent sean consistentes.
        */
        SELECT OrganizationId, RoleAttendanceEventid, OrganizationCalendarSession.OrganizationCalendarSessionId
        FROM Organization
        INNER JOIN RefOrganizationType USING(RefOrganizationTypeId)
        INNER JOIN OrganizationCalendar USING(OrganizationId)
        INNER JOIN OrganizationCalendarSession USING(OrganizationCalendarId)
        INNER JOIN OrganizationPersonRole USING(OrganizationId)
        INNER JOIN RoleAttendanceEvent USING(OrganizationPersonRoleId)
        WHERE
        RefOrganizationType.Description IN ('Course Section')
        AND
        AttendanceTermIndicator = 1
      """).fetchall()
    except Exception as e:
      logger.info(f"Resultado: {listInfoSuccesfull} -> {str(e)}")

    if(len(listInfoSuccesfull) <= 0):
      logger.info("S/Datos")
      _r = True
      return_dict[getframeinfo(currentframe()).function] = _r
      logger.info(f"{current_process().name} finalizando...")      
      return _r

    RoleAttendance = []
    try:
      RoleAttendance = conn.execute("""
        /*
        * verifica que los registro de calendar Session y RoleAttendanceEvent sean consistentes.
        */
        SELECT OrganizationId, r.RoleAttendanceEventid, OrganizationCalendarSession.OrganizationCalendarSessionId
        FROM (
          SELECT 
              OrganizationId
            , RoleAttendanceEventid
            , AttendanceTermIndicator
            , OrganizationCalendarSession.OrganizationCalendarSessionId
            , DATETIME(DATE(BeginDate) || 'T' || TIME(SessionStartTime)) as 'InicioClase'
            , RoleAttendanceEvent.Date
            , DATETIME(DATE(EndDate) || 'T' || TIME(SessionEndTime)) as 'FinClase'
            , *
          FROM Organization
          OUTER LEFT JOIN RefOrganizationType USING(RefOrganizationTypeId)
          OUTER LEFT JOIN OrganizationCalendar USING(OrganizationId)
          OUTER LEFT JOIN OrganizationCalendarSession USING(OrganizationCalendarId)
          OUTER LEFT JOIN OrganizationPersonRole USING(OrganizationId)
          OUTER LEFT JOIN RoleAttendanceEvent USING(OrganizationPersonRoleId)
          WHERE
          RefOrganizationType.Description IN ('Course Section')
          AND
          InicioClase = RoleAttendanceEvent.Date
          AND AttendanceTermIndicator = 1
        ) as r
        INNER JOIN RefOrganizationType USING(RefOrganizationTypeId)
        INNER JOIN OrganizationCalendar USING(OrganizationId)
        INNER JOIN OrganizationCalendarSession USING(OrganizationCalendarId)
        INNER JOIN OrganizationPersonRole USING(OrganizationId)
        INNER JOIN RoleAttendanceEvent USING(OrganizationPersonRoleId)
      """).fetchall()
    except Exception as e:
      logger.info(f"Resultado: {RoleAttendance} -> {str(e)}")
    
    logger.info(f"Eventos mal identificados: {len(RoleAttendance)}")  
    try:
        if(len(RoleAttendance)>0):
          data1 = list(set([m[0] for m in RoleAttendance if m[0] is not None]))
          _c1 = len(set(data1))
          _err1 = f"Las siguientes organizaciones no coinciden: {data1}"
          if (_c1 > 0):
            logger.error(_err1)
            logger.error(f"Rechazado")
        else:
          logger.info(f"Aprobado")
          _r = True
    except Exception as e:
      logger.error(f"NO se pudo ejecutar la consulta a la verificación: {str(e)}")
      logger.error(f"Rechazado")
    finally:
      return_dict[getframeinfo(currentframe()).function] = _r
      logger.info(f"{current_process().name} finalizando...")      
      return _r
