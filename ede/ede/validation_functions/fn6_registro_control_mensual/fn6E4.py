from inspect import getframeinfo, currentframe
from multiprocessing import current_process

from ede.ede._logger import logger


def fn6E4(conn, return_dict):
    """
    REGISTRO CONTROL MENSUAL DE ASISTENCIA O CONTROL DE SUBVENCIONES
    6.2 Contenido mínimo, letra c.2
    verificar que se encuentren bien registrados los cambios de 
    actividades al calendario escolar.
    Args:
        conn ([sqlalchemy.engine.Connection]): [
          Objeto que establece la conexión con la base de datos.
          Creado previamente a través de la función execute(self)
          ]
    Returns:
        [Boolean]: [          Retorna True/False y "S/Datos" a través de logger, solo si puede:
            - A
          Retorna True y “Aprobado” a través de logger, solo si se puede: 
            - A
          En todo otro caso, retorna False y "Rechazado" a través de logger.
          ]
    """
    try:
        _data = []
        _data = conn.execute("""
                SELECT 
                  org
                  ,group_concat(DISTINCT diasSinClases) as 'diasSinClases'
                FROM (
                  WITH RECURSIVE dates(Organizationid, date) AS (
                    SELECT Organizationid, StartDate
                    FROM OrganizationCalendarCrisis O
                    UNION ALL
                    SELECT Organizationid, date(date, '+1 day')
                    FROM dates
                    WHERE 
                    -- Considera la menor fecha entre LastInstructionDate y la fecha actual (now)
                    strftime('%Y-%m-%d',date) < strftime('%Y-%m-%d', ( 
                      -- Rescata el último día sin actividades producto de la crisis
                      SELECT EndDate 
                      FROM OrganizationCalendarCrisis occ
                      WHERE occ.OrganizationId = Organizationid
                      )
                    ) 
                  )
                  SELECT Organizationid as 'org',  group_concat(date) as 'diasSinClases'
                  FROM dates
                  GROUP BY OrganizationId

                  UNION ALL

                  SELECT oc.Organizationid as 'org', group_concat(oce.EventDate) as 'diasSinClases'
                  FROM OrganizationCalendarEvent oce
                  JOIN OrganizationCalendar oc
                    ON oce.OrganizationCalendarId = oc.OrganizationCalendarId
                  JOIN RefCalendarEventType rcet
                    ON oce.RefCalendarEventType = rcet.RefCalendarEventTypeId
                    AND rcet.Code IN ('EmergencyDay','Holiday','Strike','TeacherOnlyDay')	
                  GROUP BY oc.Organizationid
                ) DSC
                GROUP BY org      
      """).fetchall()

        if(not _data):
            logger.error(f"S/Datos")
            return_dict[getframeinfo(currentframe()).function] = True
            logger.info(f"{current_process().name} finalizando...")
            return True

    except:
        logger.error(f"S/Datos")
        return_dict[getframeinfo(currentframe()).function] = True
        logger.info(f"{current_process().name} finalizando...")
        return True

    try:
        _result = []
        _result = conn.execute("""
--  6.2 Contenido mínimo, letra c.2
-- verificar que se encuentren bien registrados los cambios de actividades al calendario escolar.
-- las tablas OrganizationCalendarEvent y OrganizationCalendarCrisis guardan los casos de suspensión
-- excepcionales de clases, por lo tanto, se debe verificar que existan consistencia entre 
-- la suspensión de clases y las clases realizadas.
-- *** Los días de suspensión no deberían existir registros de clases o asistencias ***

SELECT 
	org
	,group_concat(DISTINCT diasSinClases) as 'diasSinClases'
	,group_concat(DISTINCT clases.inicioClase) as 'diasInicioClases'
	,group_concat(DISTINCT clases.finClase) as 'diasfinClases'
	,group_concat(DISTINCT clases.fechaAsistencia) as 'diasFechaAsistencia'
FROM (
	WITH RECURSIVE dates(Organizationid, date) AS (
	  SELECT Organizationid, StartDate
	  FROM OrganizationCalendarCrisis O
	  UNION ALL
	  SELECT Organizationid, date(date, '+1 day')
	  FROM dates
	  WHERE 
		-- Considera la menor fecha entre LastInstructionDate y la fecha actual (now)
		strftime('%Y-%m-%d',date) < strftime('%Y-%m-%d', ( 
			-- Rescata el último día sin actividades producto de la crisis
			SELECT EndDate 
			FROM OrganizationCalendarCrisis occ
			WHERE occ.OrganizationId = Organizationid
			)
		) 
	)
	SELECT Organizationid as 'org',  group_concat(date) as 'diasSinClases'
	FROM dates
	GROUP BY OrganizationId

	UNION ALL

	SELECT oc.Organizationid as 'org', group_concat(oce.EventDate) as 'diasSinClases'
	FROM OrganizationCalendarEvent oce
	JOIN OrganizationCalendar oc
		ON oce.OrganizationCalendarId = oc.OrganizationCalendarId
	JOIN RefCalendarEventType rcet
		ON oce.RefCalendarEventType = rcet.RefCalendarEventTypeId
		AND rcet.Code IN ('EmergencyDay','Holiday','Strike','TeacherOnlyDay')	
	GROUP BY oc.Organizationid
) DSC
JOIN Organization O
	ON org = O.OrganizationId
JOIN RefOrganizationType rot
	ON rot.RefOrganizationTypeId = O.RefOrganizationTypeId
	AND rot.code IN ('CourseSection')
JOIN (
	SELECT DISTINCT
		 O.OrganizationId
		,ocs.BeginDate||"T"||ifnull(ocs.SessionStartTime,"00:00:00") as 'InicioClase'
		,ocs.EndDate||"T"||ifnull(ocs.SessionEndTime,"00:00:00") as 'finClase'
		,rat.Date as 'fechaAsistencia'
		,rat.digitalRandomKeyDate as 'fechafirma'
		,CASE WHEN (rat.Date BETWEEN ocs.BeginDate||"T"||ifnull(ocs.SessionStartTime,"00:00:00") AND ocs.EndDate||"T"||ifnull(ocs.SessionEndTime,"00:00:00")) THEN 'True' ELSE 'False' END as 'rangoHorarioCorrecto'
		,CASE WHEN (rat.digitalRandomKeyDate BETWEEN ocs.BeginDate||"T"||ifnull(ocs.SessionStartTime,"00:00:00") AND ocs.EndDate||"T"||ifnull(ocs.SessionEndTime,"00:00:00")) THEN 'True' ELSE 'False' END as 'rangoFirmaCorrecto'
	FROM Organization O
	JOIN RefOrganizationType rot
		ON rot.RefOrganizationTypeId = O.RefOrganizationTypeId
		AND rot.code IN ('CourseSection')
	JOIN OrganizationCalendar oc
		ON oc.OrganizationId = O.OrganizationId
		AND oc.RecordEndDateTime IS NULL
	LEFT JOIN OrganizationCalendarSession ocs
		ON ocs.OrganizationCalendarId = oc.OrganizationCalendarId
		AND ocs.AttendanceTermIndicator = 1
		AND ocs.RecordEndDateTime IS NULL
	LEFT JOIN OrganizationPersonRole opr
		ON opr.OrganizationId = O.OrganizationId
		AND opr.RecordEndDateTime IS NULL
	LEFT JOIN RoleAttendanceEvent rat
		ON rat.OrganizationPersonRoleId = opr.OrganizationPersonRoleId
		AND rat.RecordEndDateTime IS NULL
) clases 
	ON clases.OrganizationId = org
	AND (
		(DSC.diasSinClases) LIKE "%" || strftime("%Y-%m-%d",clases.InicioClase) || "%"
		OR
		(DSC.diasSinClases) LIKE "%" || strftime("%Y-%m-%d",clases.finClase) || "%"
		OR
		(DSC.diasSinClases) LIKE "%" || strftime("%Y-%m-%d",clases.fechaAsistencia) || "%"
	)

GROUP BY org      
      """).fetchall()
    except:
        pass
    try:
        if(not _result):
            logger.info(f"Aprobado")
            return_dict[getframeinfo(currentframe()).function] = True
            logger.info(f"{current_process().name} finalizando...")
            return True

        organizacionesErrors = []
        fechasSesionesErrors = []
        fechasAsistenciasErrors = []

        for row in _result:
            organizacion = row[0]
            fechasSesion = row[2]
            fechasAsistencia = row[4]

            if(fechasSesion):
                organizacionesErrors.append(organizacion)
                fechasSesionesErrors.append(fechasSesion)

            if(fechasAsistencia):
                organizacionesErrors.append(organizacion)
                fechasAsistenciasErrors.append(fechasAsistencia)

        if(fechasAsistenciasErrors):
            logger.error(
                f"Fechas de asistencia de la tabla roleAttendanceEvent en fechas catalogadas como sin clases: {str(set(fechasAsistenciasErrors))}")

        if(fechasSesionesErrors):
            logger.error(
                f"Fechas de sesiones de la tabla OrganizationCalendarSession en fechas catalogadas como sin clases: {str(set(fechasSesionesErrors))}")

        if(organizacionesErrors):
            logger.error(
                f"Las siguientes organizaciones estan con problemas: {str(set(organizacionesErrors))}")
            logger.error(f"Rechazado")
            return_dict[getframeinfo(currentframe()).function] = False
            logger.info(f"{current_process().name} finalizando...")
            return False

    except Exception as e:
        logger.error(
            f"NO se pudo ejecutar la consulta de entrega de informaciÓn: {str(e)}")
        logger.error(f"Rechazado")
        return_dict[getframeinfo(currentframe()).function] = False
        logger.info(f"{current_process().name} finalizando...")
        return False
### fin  fn6E4 ###
