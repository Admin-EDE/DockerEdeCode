from inspect import getframeinfo, currentframe
from multiprocessing import current_process

from ede.ede.validation_functions.check_bd_utils import ejecutar_sql
from ede.ede._logger import logger


def fn6B0(conn, return_dict):
    """
    REGISTRO CONTROL MENSUAL DE ASISTENCIA O CONTROL DE SUBVENCIONES
    6.2 Contenido mínimo, letra c.8
    Todas las correcciones realizadas al registro de asistencia y asignatura se registren indicando su fecha, hora, verificador de identidad del funcionario que la realiza dicha acción y motivo del cambio.y estan visadas por el director del establecimiento o el funcionario que él haya designado.
    Args:
        conn ([sqlalchemy.engine.Connection]): [
          Objeto que establece la conexión con la base de datos.
          Creado previamente a través de la función execute(self)
          ]
    Returns:
        [Boolean]: [ Retorna True/False y "S/Datos" a través de logger, solo si puede:
            - No hay correcciones al registro de asistencia
          Retorna True y “Aprobado” a través de logger, solo si se puede: 
            - Las correcciones al registro de asistencia tienen todos los datos mínimos y están visadas.
          En todo otro caso, retorna False y "Rechazado" a través de logger.
          ]
    """
    _r = False    
    _rightList = []
    try:
      _rightList = ejecutar_sql(conn, """--sql
	WITH RECURSIVE cte_Attendance (RoleAttendanceEventId, OrganizationPersonRoleId, RUN, Fecha, RecordEndDateTime) AS (
		SELECT 
			 rae.RoleAttendanceEventId
			,rae.OrganizationPersonRoleId
			,pid.Identifier as 'RUN'
			,rae.Date
			,rae.RecordEndDateTime
		FROM RoleAttendanceEvent rae
		JOIN OrganizationPersonRole opr 
			on rae.OrganizationPersonRoleId = opr.OrganizationPersonRoleId 
		JOIN RefAttendanceEventType raet
			ON rae.RefAttendanceEventTypeId = raet.RefAttendanceEventTypeId
			AND raet.Code IN ('DailyAttendance','ClassSectionAttendance')
		JOIN PersonIdentifier pid
			ON opr.personid = pid.personid 
		JOIN RefPersonIdentificationSystem rpis
			ON pid.RefPersonIdentificationSystemId = rpis.RefPersonIdentificationSystemId
			AND rpis.Code IN ('RUN')
		JOIN role rol_e
			ON opr.RoleId = rol_e.RoleId
			AND rol_e.Name IN ('Estudiante')
		WHERE 
			rae.RecordEndDateTime IS NOT NULL
			AND
			rae.RecordStartDateTime IS NOT NULL
			AND
			rae.oprIdRatificador IS NULL
			AND
			rae.firmaRatificador IS NULL
			AND 
			rae.fechaRatificador IS NULL
			AND
            -- Agrega a la lista todos los registros que cumplen con la expresión regular
            rae.Date REGEXP '^(19|2[0-9])[0-9]{2}-(0[1-9]|1[012])-(0[1-9]|[12][0-9]|3[01])([T ])(0[0-9]|1[0-9]|2[0-3]):([0-5][0-9]):([0-5][0-9])(.[0-9]+)?((\+|\-)(0[0-9]|1[0-9]|2[0-3]):([0-5][0-9]))$'
            AND
            -- Agrega a la lista todos los registros que cumplen con la expresión regular
            rae.digitalRandomKeyDate REGEXP '^(19|2[0-9])[0-9]{2}-(0[1-9]|1[012])-(0[1-9]|[12][0-9]|3[01])([T ])(0[0-9]|1[0-9]|2[0-3]):([0-5][0-9]):([0-5][0-9])(.[0-9]+)?((\+|\-)(0[0-9]|1[0-9]|2[0-3]):([0-5][0-9]))$'			
		
		UNION ALL

		SELECT 
			rae.RoleAttendanceEventId
			,rae.OrganizationPersonRoleId
			,pid.Identifier as 'RUN'
			,rae.Date, rae.RecordEndDateTime
		FROM RoleAttendanceEvent rae
		JOIN cte_Attendance cte 
			ON cte.RecordEndDateTime = rae.Date
			AND cte.OrganizationPersonRoleId = rae.OrganizationPersonRoleId
			AND rae.RecordStartDateTime IS NOT NULL
		JOIN OrganizationPersonRole opr 
			on rae.OrganizationPersonRoleId = opr.OrganizationPersonRoleId 
		JOIN RefAttendanceEventType raet
			ON rae.RefAttendanceEventTypeId = raet.RefAttendanceEventTypeId
			AND raet.Code IN ('DailyAttendance','ClassSectionAttendance')
		JOIN PersonIdentifier pid
			ON opr.personid = pid.personid 
		JOIN RefPersonIdentificationSystem rpis
			ON pid.RefPersonIdentificationSystemId = rpis.RefPersonIdentificationSystemId
			AND rpis.Code IN ('RUN')		
		JOIN role rol_e
			ON opr.RoleId = rol_e.RoleId
			AND rol_e.Name IN ('Estudiante')
		JOIN OrganizationPersonRole opr_ratificador 
			ON rae.oprIdRatificador = opr_ratificador.OrganizationPersonRoleId 
		JOIN role rol_ratificador
			ON opr_ratificador.RoleId = rol_ratificador.RoleId
			AND rol_ratificador.Name IN ('Encargado de la asistencia','Director(a)')
		WHERE 
			-- Agrega a la lista todos los registros que cumplen con la expresión regular
			rae.Date REGEXP '^(19|2[0-9])[0-9]{2}-(0[1-9]|1[012])-(0[1-9]|[12][0-9]|3[01])([T ])(0[0-9]|1[0-9]|2[0-3]):([0-5][0-9]):([0-5][0-9])(.[0-9]+)?((\+|\-)(0[0-9]|1[0-9]|2[0-3]):([0-5][0-9]))$'
			AND
			-- Agrega a la lista todos los registros que cumplen con la expresión regular
			rae.digitalRandomKeyDate REGEXP '^(19|2[0-9])[0-9]{2}-(0[1-9]|1[012])-(0[1-9]|[12][0-9]|3[01])([T ])(0[0-9]|1[0-9]|2[0-3]):([0-5][0-9]):([0-5][0-9])(.[0-9]+)?((\+|\-)(0[0-9]|1[0-9]|2[0-3]):([0-5][0-9]))$'			
            AND
            -- Agrega a la lista todos los registros que cumplen con la expresión regular
            rae.fechaRatificador REGEXP '^(19|2[0-9])[0-9]{2}-(0[1-9]|1[012])-(0[1-9]|[12][0-9]|3[01])([T ])(0[0-9]|1[0-9]|2[0-3]):([0-5][0-9]):([0-5][0-9])(.[0-9]+)?((\+|\-)(0[0-9]|1[0-9]|2[0-3]):([0-5][0-9]))$'			
            AND
			-- Agrega a la lista todos los registros que no cumplan con la expresión regular
            rae.digitalRandomKey REGEXP '^[0-9]{6}([-]{1}[0-9kK]{1})?$'
            AND
			-- Agrega a la lista todos los registros que no cumplan con la expresión regular
            rae.firmaRatificador REGEXP '^[0-9]{6}([-]{1}[0-9kK]{1})?$'			

	)
	SELECT 
		group_concat(RoleAttendanceEventId) as 'roleAttendanceEventIds'
		,OrganizationPersonRoleId
		,RUN
		,min(Fecha) as 'PRIMERA_FECHA_REGISTRADA'
		,max(fecha) as 'ULTIMA_FECHA_REGISTRADA'
	FROM cte_Attendance 
      """)
    except:
      logger.info(f"Resultado: {_rightList} -> {str(e)}")

    _errorsList = []
    try:
      _errorsList = ejecutar_sql(conn, """--sql
SELECT *
FROM RoleAttendanceEvent rae
JOIN (
	WITH RECURSIVE cte_Attendance (RoleAttendanceEventId, OrganizationPersonRoleId, RUN, Fecha, RecordEndDateTime) AS (
		SELECT 
			 rae.RoleAttendanceEventId
			,rae.OrganizationPersonRoleId
			,pid.Identifier as 'RUN'
			,rae.Date
			,rae.RecordEndDateTime
		FROM RoleAttendanceEvent rae
		JOIN OrganizationPersonRole opr 
			on rae.OrganizationPersonRoleId = opr.OrganizationPersonRoleId 
		JOIN RefAttendanceEventType raet
			ON rae.RefAttendanceEventTypeId = raet.RefAttendanceEventTypeId
			AND raet.Code IN ('DailyAttendance','ClassSectionAttendance')
		JOIN PersonIdentifier pid
			ON opr.personid = pid.personid 
		JOIN RefPersonIdentificationSystem rpis
			ON pid.RefPersonIdentificationSystemId = rpis.RefPersonIdentificationSystemId
			AND rpis.Code IN ('RUN')
		JOIN role rol_e
			ON opr.RoleId = rol_e.RoleId
			AND rol_e.Name IN ('Estudiante')
		WHERE 
			rae.RecordEndDateTime IS NOT NULL
			AND
			rae.RecordStartDateTime IS NOT NULL
			AND
			rae.oprIdRatificador IS NULL
			AND
			rae.firmaRatificador IS NULL
			AND 
			rae.fechaRatificador IS NULL
			AND
            -- Agrega a la lista todos los registros que cumplen con la expresión regular
            rae.Date REGEXP '^(19|2[0-9])[0-9]{2}-(0[1-9]|1[012])-(0[1-9]|[12][0-9]|3[01])([T ])(0[0-9]|1[0-9]|2[0-3]):([0-5][0-9]):([0-5][0-9])(.[0-9]+)?((\+|\-)(0[0-9]|1[0-9]|2[0-3]):([0-5][0-9]))$'
            AND
            -- Agrega a la lista todos los registros que cumplen con la expresión regular
            rae.digitalRandomKeyDate REGEXP '^(19|2[0-9])[0-9]{2}-(0[1-9]|1[012])-(0[1-9]|[12][0-9]|3[01])([T ])(0[0-9]|1[0-9]|2[0-3]):([0-5][0-9]):([0-5][0-9])(.[0-9]+)?((\+|\-)(0[0-9]|1[0-9]|2[0-3]):([0-5][0-9]))$'			
		
		UNION ALL

		SELECT 
			rae.RoleAttendanceEventId
			,rae.OrganizationPersonRoleId
			,pid.Identifier as 'RUN'
			,rae.Date, rae.RecordEndDateTime
		FROM RoleAttendanceEvent rae
		JOIN cte_Attendance cte 
			ON cte.RecordEndDateTime = rae.Date
			AND cte.OrganizationPersonRoleId = rae.OrganizationPersonRoleId
			AND rae.RecordStartDateTime IS NOT NULL
		JOIN OrganizationPersonRole opr 
			on rae.OrganizationPersonRoleId = opr.OrganizationPersonRoleId 
		JOIN RefAttendanceEventType raet
			ON rae.RefAttendanceEventTypeId = raet.RefAttendanceEventTypeId
			AND raet.Code IN ('DailyAttendance','ClassSectionAttendance')
		JOIN PersonIdentifier pid
			ON opr.personid = pid.personid 
		JOIN RefPersonIdentificationSystem rpis
			ON pid.RefPersonIdentificationSystemId = rpis.RefPersonIdentificationSystemId
			AND rpis.Code IN ('RUN')		
		JOIN role rol_e
			ON opr.RoleId = rol_e.RoleId
			AND rol_e.Name IN ('Estudiante')
		JOIN OrganizationPersonRole opr_ratificador 
			ON rae.oprIdRatificador = opr_ratificador.OrganizationPersonRoleId 
		JOIN role rol_ratificador
			ON opr_ratificador.RoleId = rol_ratificador.RoleId
			AND rol_ratificador.Name IN ('Encargado de la asistencia','Director(a)')
		WHERE 
			-- Agrega a la lista todos los registros que cumplen con la expresión regular
			rae.Date REGEXP '^(19|2[0-9])[0-9]{2}-(0[1-9]|1[012])-(0[1-9]|[12][0-9]|3[01])([T ])(0[0-9]|1[0-9]|2[0-3]):([0-5][0-9]):([0-5][0-9])(.[0-9]+)?((\+|\-)(0[0-9]|1[0-9]|2[0-3]):([0-5][0-9]))$'
			AND
			-- Agrega a la lista todos los registros que cumplen con la expresión regular
			rae.digitalRandomKeyDate REGEXP '^(19|2[0-9])[0-9]{2}-(0[1-9]|1[012])-(0[1-9]|[12][0-9]|3[01])([T ])(0[0-9]|1[0-9]|2[0-3]):([0-5][0-9]):([0-5][0-9])(.[0-9]+)?((\+|\-)(0[0-9]|1[0-9]|2[0-3]):([0-5][0-9]))$'			
            AND
            -- Agrega a la lista todos los registros que cumplen con la expresión regular
            rae.fechaRatificador REGEXP '^(19|2[0-9])[0-9]{2}-(0[1-9]|1[012])-(0[1-9]|[12][0-9]|3[01])([T ])(0[0-9]|1[0-9]|2[0-3]):([0-5][0-9]):([0-5][0-9])(.[0-9]+)?((\+|\-)(0[0-9]|1[0-9]|2[0-3]):([0-5][0-9]))$'			
            AND
			-- Agrega a la lista todos los registros que no cumplan con la expresión regular
            rae.digitalRandomKey REGEXP '^[0-9]{6}([-]{1}[0-9kK]{1})?$'
            AND
			-- Agrega a la lista todos los registros que no cumplan con la expresión regular
            rae.firmaRatificador REGEXP '^[0-9]{6}([-]{1}[0-9kK]{1})?$'			

	)
	SELECT 
		group_concat(RoleAttendanceEventId) as 'roleAttendanceEventIds'
		,OrganizationPersonRoleId
		,RUN
		,min(Fecha) as 'PRIMERA_FECHA_REGISTRADA'
		,max(fecha) as 'ULTIMA_FECHA_REGISTRADA'
	FROM cte_Attendance 
) result ON (result.roleAttendanceEventIds) NOT LIKE '%' || rae.RoleAttendanceEventId || '%'
LEFT JOIN OrganizationPersonRole opr 
	on rae.OrganizationPersonRoleId = opr.OrganizationPersonRoleId 
LEFT JOIN RefAttendanceEventType raet
	ON rae.RefAttendanceEventTypeId = raet.RefAttendanceEventTypeId
	AND raet.Code IN ('DailyAttendance','ClassSectionAttendance')
LEFT JOIN role rol_e
	ON opr.RoleId = rol_e.RoleId
	AND rol_e.Name IN ('Estudiante')
WHERE
	rae.RecordEndDateTime IS NOT NULL
	OR
	rae.oprIdRatificador IS NOT NULL
	OR
	rae.firmaRatificador IS NOT NULL
	OR
	rae.fechaRatificador IS NOT NULL
	OR 
	rae.RecordStartDateTime != Date	
      """)
    except:
      logger.info(f"Resultado: {_errorsList} -> {str(e)}")
    
    try:
      _ids = (list([m[0] for m in _rightList if m[0] is not None]))      
      if(not _errorsList and not _ids):
        logger.info(f"S/Datos")
        return_dict[getframeinfo(currentframe()).function] = True
        logger.info(f"{current_process().name} finalizando...")
        return True

      if(not _errorsList and _ids):
        logger.info(f"APROBADO")
        return_dict[getframeinfo(currentframe()).function] = True
        logger.info(f"{current_process().name} finalizando...")
        return True

      roleAttendanceEventIds = (list([m[0] for m in _errorsList if m[0] is not None]))
      logger.error(f"Los siguientes roleAttendanceEvent Ids estan con problemas: {str(roleAttendanceEventIds)}")
      logger.error(f"Rechazado")
      return_dict[getframeinfo(currentframe()).function] = False
      logger.info(f"{current_process().name} finalizando...")
      return False
    except Exception as e:
      logger.error(f"NO se pudo ejecutar la consulta de entrega de informaciÓn: {str(e)}")
      logger.error(f"Rechazado")
      return_dict[getframeinfo(currentframe()).function] = False
      logger.info(f"{current_process().name} finalizando...")
      return False