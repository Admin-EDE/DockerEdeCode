from inspect import getframeinfo, currentframe
from multiprocessing import current_process


from ede.ede._logger import logger


  # Revisar que los cursos del establecimiento tengan bien 
  # calculada la información de la tabla RoleAttendance.
def fn3DA(conn, return_dict):
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
        SELECT RoleAttendanceId
        FROM RoleAttendance
        OUTER LEFT JOIN OrganizationPersonRole USING(OrganizationPersonRoleId)
        OUTER LEFT JOIN Organization USING(OrganizationId)
        OUTER LEFT JOIN RefOrganizationType USING(RefOrganizationTypeId)
        OUTER LEFT JOIN Role USING(RoleId)
        OUTER LEFT JOIN RoleAttendanceEvent USING(OrganizationPersonRoleId)
        OUTER LEFT JOIN RefAttendanceStatus USING(RefAttendanceStatusId)
        OUTER LEFT JOIN RefAttendanceEventType USING(RefAttendanceEventTypeId)
        WHERE
        RefOrganizationType.Description IN ('Course')
        AND
        Role.Name IN ('Estudiante') -- filtra la asistencia de los estudiantes
        AND
        RefAttendanceEventType.Description IN ('Daily attendance') -- Filtra la asistencia diaria      
      """).fetchall()
    except Exception as e:
      logger.info(f"Resultado: {listInfoSuccesfull} -> {str(e)}")

    if(len(listInfoSuccesfull)<=0):
      logger.info("S/Datos")
      _r = True
      return_dict[getframeinfo(currentframe()).function] = _r
      logger.info(f"{current_process().name} finalizando...")
      return _r
    RoleAttendance = []
    try:
      RoleAttendance = conn.execute("""
        /*
        * Lista los registros de la Tabla RoleAttendance que no coinciden 
        * con la lista de eventos de asistencia regitrados en la tabla RoleAttendanceEvent
        */
        SELECT 
          ra.RoleAttendanceId,
          ifnull(ra.AttendanceRate, 0) as 'AttendanceRate_o',
          ifnull(result.AttendanceRate, 0) as 'AttendanceRate_r', 
          ifnull(ra.NumberOfDaysInAttendance, 0) as 'NumberOfDaysInAttendance_o', 
          ifnull(result.NumberOfDaysInAttendance, 0) as 'NumberOfDaysInAttendance_r', 
          ifnull(ra.NumberOfDaysAbsent, 0) as 'NumberOfDaysAbsent_o', 
          ifnull(result.NumberOfDaysAbsent, 0) as 'NumberOfDaysAbsent_r', 
          ifnull(result.totalDays, 0) as 'totalDays_r'
        FROM RoleAttendance as ra
        OUTER LEFT JOIN OrganizationPersonRole USING(OrganizationPersonRoleId)
        OUTER LEFT JOIN Organization USING(OrganizationId)
        OUTER LEFT JOIN RefOrganizationType USING(RefOrganizationTypeId)
        LEFT JOIN (
        -- Calcula el campo AttendanceRate a partir de la información de la tabla RoleAttendanceEvent
        SELECT *, CASE WHEN NumberOfDaysInAttendance NOT NULL THEN CAST(NumberOfDaysInAttendance as real) / cast(totalDays AS REAL) * 100 ELSE 0.00 END as 'AttendanceRate'
          FROM (
            -- Agrupando la información por estudiante, se cuenta los días presentes y ausentes de cada uno
            SELECT RoleAttendanceId,PersonId, RefOrganizationType.Description as 'OrganizationType', 
              sum(
                CASE RefAttendanceStatus.Description 
                  WHEN 'Present' THEN 1 ELSE 0 END
              ) as 'NumberOfDaysInAttendance',
              sum(
                CASE WHEN RefAttendanceStatus.Description like '%Absence%' THEN 1 ELSE 0 END
              ) as 'NumberOfDaysAbsent',
              count(personId) as 'totalDays'
            FROM RoleAttendance
            OUTER LEFT JOIN OrganizationPersonRole USING(OrganizationPersonRoleId)
            OUTER LEFT JOIN Organization USING(OrganizationId)
            OUTER LEFT JOIN RefOrganizationType USING(RefOrganizationTypeId)
            OUTER LEFT JOIN Role USING(RoleId)
            OUTER LEFT JOIN RoleAttendanceEvent USING(OrganizationPersonRoleId)
            OUTER LEFT JOIN RefAttendanceStatus USING(RefAttendanceStatusId)
            OUTER LEFT JOIN RefAttendanceEventType USING(RefAttendanceEventTypeId)
            WHERE 
            Role.Name IN ('Estudiante') -- filtra la asistencia de los estudiantes
            AND
            RefAttendanceEventType.Description IN ('Daily attendance') -- Filtra la asistencia diaria
            AND
            RefOrganizationType.Description IN ('Course') -- Filtra que la asistencia diaria se reporte a nivel de curso
            GROUP BY personId
          )) as result
        ON 
          ra.RoleAttendanceId = result.RoleAttendanceId
        WHERE 
        RefOrganizationType.Description IN ('Course') AND
        -- Filtra solo aquellos casos en que la información no coincide
        NOT (AttendanceRate_o = AttendanceRate_r AND NumberOfDaysInAttendance_o = NumberOfDaysInAttendance_r AND NumberOfDaysAbsent_o = NumberOfDaysAbsent_r)
      """).fetchall()
    except Exception as e:
      logger.info(f"Resultado: {RoleAttendance} -> {str(e)}")
    
    logger.info(f"Localidades mal asignadas: {len(RoleAttendance)}")      
    try:
        if( len(RoleAttendance) > 0 ):
          data1 = list(set([m[0] for m in RoleAttendance if m[0] is not None]))
          _c1 = len(set(data1))
          _err1 = f"Los siguientes organizaciones no tienen sus AttendanceRate bien calculados: {data1}"
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
