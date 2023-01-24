from inspect import getframeinfo, currentframe
from multiprocessing import current_process
import sys

from ede.ede.validation_functions.check_bd_utils import ejecutar_sql
from ede.ede._logger import logger


def fn5F0(conn, return_dict):
    """
    REGISTRO DE CONTROL DE ASIGNATURA
    6.2 Contenido mínimo, letra b.1
    La información relacionada con el cumplimiento de los programas de estudio y asistencia de los estudiantes es válida.
    Args:
        conn ([sqlalchemy.engine.Connection]): [
          Objeto que establece la conexión con la base de datos.
          Creado previamente a través de la función execute(self)
          ]
    Returns:
        [Boolean]: [
          Retorna True/False y "S/Datos" a través de logger, solo si puede:
            - No hay fechas de clases
          Retorna True y “Aprobado” a través de logger, solo si se puede: 
            -- * día de clases
            -- * mes respectivo
            -- * hora pedagógica
            -- * nombre de la asignatura o sector
            -- * total de estudiantes presentes, atrasados y ausentes
            -- * observaciones de la clase
            -- * Verificador de identidad del docente a cargo

            -- Lee desde las organizaciones de tipo asignatura los campos 
            -- FirstInstructionDate y LastInstructionDate y con esa información
            -- crea una lista de días hábiles en los cuales deberían haber tenido clases
            -- los estudiantes del establecimiento.
          En todo otro caso, retorna False y "Rechazado" a través de logger.
          ]
    """
    _r = False
    _ExistData = []
    try:
        _ExistData = ejecutar_sql(conn, """--sql
WITH RECURSIVE dates(Organizationid, date) AS (
  -------------------------------------------------------------------------------------------------------------------------------------------------------------------
  SELECT 
    O.Organizationid
    , FirstInstructionDate
  FROM Organization O
  JOIN RefOrganizationType rot
    ON O.RefOrganizationTypeId = rot.RefOrganizationTypeId
    AND O.RefOrganizationTypeId IN 
      (
        SELECT RefOrganizationTypeId 
        FROM RefOrganizationType
        WHERE Description IN ('Course Section')
      ) 
  JOIN OrganizationCalendar oc
    --ON oc.OrganizationCalendarId = ocs.OrganizationCalendarId
    	ON oc.OrganizationId = o.OrganizationId
  JOIN OrganizationCalendarSession ocs
    ON oc.OrganizationCalendarId = ocs.OrganizationCalendarId
    AND ocs.FirstInstructionDate NOT NULL
  -------------------------------------------------------------------------------------------------------------------------------------------------------------------
  UNION ALL
  SELECT Organizationid, date(date, '+1 day')
  FROM dates
  WHERE 
  -- Considera la menor fecha entre LastInstructionDate y la fecha actual (now)
  strftime('%Y-%m-%d',date) < strftime('%Y-%m-%d', 
    ( 
    -- Rescata el último día 
    SELECT LastInstructionDate 
    FROM OrganizationCalendarSession ocs 
    JOIN OrganizationCalendar oc 
      ON oc.OrganizationCalendarId = ocs.OrganizationCalendarId 
      AND oc.OrganizationId = Organizationid
    WHERE ocs.LastInstructionDate NOT NULL
    )
  ) 
  AND strftime('%Y-%m-%d',date) < strftime('%Y-%m-%d','now')
) -- END RECURSIVE
SELECT * FROM dates                                   
        """)
        if(not _ExistData):
            raise Exception("No hay registros de información")
    except Exception as e:
        logger.error(f"S/Datos")
        _r = True
        logger.info(
            f'No hay información disponible para validar. Su registro es obligatorio.')
        logger.info(
            f'Si hay información en la BD, revise si esta cumpliendo con los criterios de la consulta.')
        return_dict[getframeinfo(currentframe()).function] = _r
        return _r
    try:
        asignaturas = []
        asignaturas = ejecutar_sql(conn, """--sql
--6.2 Contenido mínimo, letra b.1
--Validar la información relacionada con el cumplimiento de los programas de estudio y asistencia de los estudiantes.
-- * día de clases
-- * mes respectivo
-- * hora pedagógica
-- * nombre de la asignatura o sector
-- * total de estudiantes presentes, atrasados y ausentes
-- * observaciones de la clase
-- * Verificador de identidad del docente a cargo

-- Lee desde las organizaciones de tipo asignatura los campos 
-- FirstInstructionDate y LastInstructionDate y con esa información
-- crea una lista de días hábiles en los cuales deberían haber tenido clases
-- los estudiantes del establecimiento.
WITH RECURSIVE dates(Organizationid, date) AS (
  -------------------------------------------------------------------------------------------------------------------------------------------------------------------
  SELECT 
    O.Organizationid
    , FirstInstructionDate
  FROM Organization O
  JOIN RefOrganizationType rot
    ON O.RefOrganizationTypeId = rot.RefOrganizationTypeId
    AND O.RefOrganizationTypeId IN 
      (
        SELECT RefOrganizationTypeId 
        FROM RefOrganizationType
        WHERE Description IN ('Course Section')
      ) 
  JOIN OrganizationCalendar oc
    --ON oc.OrganizationCalendarId = ocs.OrganizationCalendarId
    	ON oc.OrganizationId = o.OrganizationId
  JOIN OrganizationCalendarSession ocs
    ON oc.OrganizationCalendarId = ocs.OrganizationCalendarId
    AND ocs.FirstInstructionDate NOT NULL
  -------------------------------------------------------------------------------------------------------------------------------------------------------------------
  UNION ALL
  SELECT Organizationid, date(date, '+1 day')
  FROM dates
  WHERE 
  -- Considera la menor fecha entre LastInstructionDate y la fecha actual (now)
  strftime('%Y-%m-%d',date) < strftime('%Y-%m-%d', 
    ( 
    -- Rescata el último día 
    SELECT LastInstructionDate 
    FROM OrganizationCalendarSession ocs 
    JOIN OrganizationCalendar oc 
      ON oc.OrganizationCalendarId = ocs.OrganizationCalendarId 
      AND oc.OrganizationId = Organizationid
    WHERE ocs.LastInstructionDate NOT NULL
    )
  ) 
  AND strftime('%Y-%m-%d',date) < strftime('%Y-%m-%d','now')
) -- END RECURSIVE
SELECT 
	Organizationid
	, date
	, result.*
	, (SELECT OrganizationId FROM Organization WHERE RefOrganizationTypeId = 10) as 'OrgSchool'
	, md.cssClassMeetingDays
	,CASE 
      WHEN strftime('%w', date) = '0' THEN 'Domingo'
      WHEN strftime('%w', date) = '1' THEN 'Lunes'
      WHEN strftime('%w', date) = '2' THEN 'Martes'
      WHEN strftime('%w', date) = '3' THEN 'Miércoles'
      WHEN strftime('%w', date) = '4' THEN 'Jueves'
      WHEN strftime('%w', date) = '5' THEN 'Viernes'
      WHEN strftime('%w', date) = '6' THEN 'Sabado'
    END as 'diaSemanaCalendar'
FROM dates 
-------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- con el OrganizationId se hace un cruce con la consulta que calcula los datos a validar
LEFT JOIN (
    -- Esta consulta rescata desde la RoleAttendanceEvent los días con asistencia y calcula 
    -- el total de estudiantes presentes, ausentes y atrasados.
    -- Además revisa si exiten las firmas del docente en cada registro y si esta cargada la 
    -- información del leccionario
    SELECT 
    O.OrganizationId as 'idAsignatura', -- [idx 0]
    O.name as 'nombreAsignatura', -- [idx 1]
    rae.Date as 'fechaClase', -- fecha completa de la clase [idx 2]
    strftime('%Y-%m-%d', rae.Date) as 'fecha', -- rescata solo la fecha desde rae.Date [idx 3]
    CASE 
      WHEN strftime('%w', rae.Date) = '0' THEN 'Domingo'
      WHEN strftime('%w', rae.Date) = '1' THEN 'Lunes'
      WHEN strftime('%w', rae.Date) = '2' THEN 'Martes'
      WHEN strftime('%w', rae.Date) = '3' THEN 'Miércoles'
      WHEN strftime('%w', rae.Date) = '4' THEN 'Jueves'
      WHEN strftime('%w', rae.Date) = '5' THEN 'Viernes'
      WHEN strftime('%w', rae.Date) = '6' THEN 'Sabado'
    END as 'diaSemana', -- rescata solo el dpia de la semana desde rae.Date [idx 4]	
    CASE 
      WHEN strftime('%m', rae.Date) = '01' THEN 'Enero'
      WHEN strftime('%m', rae.Date) = '02' THEN 'Febrero'
      WHEN strftime('%m', rae.Date) = '03' THEN 'Marzo'
      WHEN strftime('%m', rae.Date) = '04' THEN 'Abril'
      WHEN strftime('%m', rae.Date) = '05' THEN 'Mayo'
      WHEN strftime('%m', rae.Date) = '06' THEN 'Junio'
      WHEN strftime('%m', rae.Date) = '07' THEN 'Julio'
      WHEN strftime('%m', rae.Date) = '08' THEN 'Agosto'
      WHEN strftime('%m', rae.Date) = '09' THEN 'Septiembre'
      WHEN strftime('%m', rae.Date) = '10' THEN 'Octubre'
      WHEN strftime('%m', rae.Date) = '11' THEN 'Noviembre'
      WHEN strftime('%m', rae.Date) = '12' THEN 'Diciembre'		
    END as 'Mes', -- rescata solo el mes desde rae.Date [idx 5]
    strftime('%H:%M', rae.Date, substr(rae.Date,length(rae.Date)-5,6)) as 'hora', -- rescata solo la hora desde rae.Date [idx 6]
    count(rae.RoleAttendanceEventId) as 'totalEstudiantes', -- Cantidad total de estudiantes [idx 7]
    sum(CASE WHEN rae.refattendancestatusid IN (1) THEN 1 ELSE 0 END) as 'estudiantesPresentes', -- [idx 8]
    group_concat(CASE WHEN rae.refattendancestatusid IN (1) THEN Identifier END) as 'estudiantesPresentesNumLista', -- [idx 9]
    sum(CASE WHEN rae.refattendancestatusid IN (2,3) THEN 1 ELSE 0 END) as 'estudiantesAusentes', -- [idx 10]
    group_concat(CASE WHEN rae.refattendancestatusid IN (2,3) THEN Identifier END) as 'estudiantesAusentesNumLista', -- [idx 11]
    sum(CASE WHEN rae.refattendancestatusid IN (4) THEN 1 ELSE 0 END) as 'estudiantesRetrasados', -- [idx 12]
    group_concat(CASE WHEN rae.refattendancestatusid IN (4) THEN Identifier END) as 'estudiantesRetrasadosNumLista', -- [idx 13]
    count(rae.digitalRandomKey) as 'cantidadRegistrosFirmados', -- [idx 14]
    group_concat(DISTINCT '"' || ocs.description || '"') as 'observacionesLeccionario' -- [idx 15]
    FROM Organization O
    JOIN RefOrganizationType rot
      ON O.RefOrganizationTypeId = rot.RefOrganizationTypeId
      AND O.RefOrganizationTypeId IN (
        SELECT RefOrganizationTypeId 
        FROM RefOrganizationType
        WHERE Description IN ('Course Section')
      ) 
    JOIN OrganizationPersonRole opr 
      ON O.OrganizationId = opr.OrganizationId
      AND opr.RecordEndDateTime IS NULL
      AND opr.RoleId IN (
        SELECT RoleId
        FROM Role
        WHERE Name IN ('Estudiante')
      )  
    JOIN RoleAttendanceEvent rae
      ON opr.OrganizationPersonRoleId = rae.OrganizationPersonRoleId
      AND rae.RecordEndDateTime IS NULL
    JOIN PersonIdentifier pid
      ON opr.personId = pid.personId
      AND pid.refPersonIdentificationSystemId IN (
        SELECT refPersonIdentificationSystemId
        FROM refPersonIdentificationSystem
        WHERE Code IN ('listNumber')
      )
      AND pid.RecordEndDateTime IS NULL
    JOIN OrganizationCalendar oc 
      ON O.OrganizationId = oc.OrganizationId
      AND oc.RecordEndDateTime IS NULL
    JOIN OrganizationCalendarSession ocs
      ON oc.OrganizationCalendarId = ocs.OrganizationCalendarId
      AND ocs.RecordEndDateTime IS NULL
      AND ocs.BeginDate = fecha  													--AGREGADO 2022/04/08
      AND hora between ifnull(ocs.SessionStartTime,'00:00') and ifnull(ocs.SessionEndTime,"00:00")					--AGREGADO 2022/04/08  
    JOIN CourseSectionSchedule css
      ON O.OrganizationId = css.OrganizationId
      AND css.RecordEndDateTime IS NULL

    WHERE 
        -- Verifica que se encuentre cargado el leccionario
        rae.RefAttendanceEventTypeId = 2
        AND
        -- Verifica que se encuentre cargado el leccionario
        ocs.Description NOT NULL
        AND
        -- Verifica que el indicador sea True, ya que en estos casos corresponde la relación	
        ocs.AttendanceTermIndicator IN (1)
        AND
        -- Verifica que la firma se encuentre cargada en el sistema
        rae.digitalRandomKey NOT NULL
        AND
        -- Verifica que se haya especificado si es estudiante asiste presencialmente o no.
        rae.VirtualIndicator NOT NULL
        AND
        -- Verifica que día y horario de firma corresponda con calendario de la asignatura
        css.ClassMeetingDays like '%'||diaSemana||'%'
        AND
        hora between css.ClassBeginningTime and css.ClassEndingTime
        AND
        -- Agrega a la lista todos los registros que no cumplan con la expresión regular
        rae.Date REGEXP '^(19|2[0-9])[0-9]{2}-(0[1-9]|1[012])-(0[1-9]|[12][0-9]|3[01])([T ])(0[0-9]|1[0-9]|2[0-3]):([0-5][0-9]):([0-5][0-9])(.[0-9]+)?((\+|\-)(0[0-9]|1[0-9]|2[0-3]):([0-5][0-9]))$'
        AND
        -- Agrega a la lista todos los registros que no cumplan con la expresión regular
        rae.digitalRandomKeyDate REGEXP '^(19|2[0-9])[0-9]{2}-(0[1-9]|1[012])-(0[1-9]|[12][0-9]|3[01])([T ])(0[0-9]|1[0-9]|2[0-3]):([0-5][0-9]):([0-5][0-9])(.[0-9]+)?((\+|\-)(0[0-9]|1[0-9]|2[0-3]):([0-5][0-9]))$'
        AND
        -- Agrega a la lista todos los registros que no cumplan con la expresión regular
        rae.digitalRandomKey REGEXP '^[0-9]{6}([-]{1}[0-9kK]{1})?$'
    GROUP BY O.organizationId, rae.Date
) result 
ON result.idAsignatura = OrganizationId
AND result.fecha = date
-------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Rescata las fechas desde OrganizationCalendarCrisis y las saca de la lista de días hábiles
LEFT JOIN (
  WITH RECURSIVE dates(Organizationid, date) AS (
    SELECT Organizationid, StartDate
    FROM OrganizationCalendarCrisis O
    UNION ALL
    SELECT Organizationid, date(date, '+1 day')
    FROM dates
    WHERE 
    -- Considera la menor fecha entre LastInstructionDate y la fecha actual (now)
    strftime('%Y-%m-%d',date) < strftime('%Y-%m-%d', ( 
      -- Rescata el último día 
      SELECT EndDate 
      FROM OrganizationCalendarCrisis occ
      WHERE occ.OrganizationId = Organizationid
      )
    ) 
  )
  SELECT Organizationid as 'org',  group_concat(date) as 'fechasCrisis'
  FROM dates 		
) occ 
ON occ.org = OrgSchool
-------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Rescata las fechas desde OrganizationCalendarEvent asociadas a la Asignatura y las saca de la lista de días hábiles
LEFT JOIN (
  SELECT oc.Organizationid as 'org', group_concat(oce.EventDate) as 'fechasEventos'
  FROM OrganizationCalendarEvent oce
  JOIN OrganizationCalendar oc
  ON oce.OrganizationCalendarId = oc.OrganizationCalendarId
  JOIN RefCalendarEventType rcet
  ON oce.RefCalendarEventType = rcet.RefCalendarEventTypeId
  AND rcet.Code IN ('EmergencyDay','Holiday','Strike','TeacherOnlyDay')	
  GROUP BY oc.Organizationid
) oce 
ON oce.org = Organizationid
-- Rescata las fechas desde OrganizationCalendarEvent asociadas al Establecimiento y las saca de la lista de días hábiles 
LEFT JOIN (
SELECT oc.Organizationid as 'org', group_concat(oce.EventDate) as 'fechasEventos'
  FROM OrganizationCalendarEvent oce
  JOIN OrganizationCalendar oc
  ON oce.OrganizationCalendarId = oc.OrganizationCalendarId
  JOIN RefCalendarEventType rcet
  ON oce.RefCalendarEventType = rcet.RefCalendarEventTypeId
  AND rcet.Code IN ('EmergencyDay','Holiday','Strike','TeacherOnlyDay')	
  JOIN Organization o
  ON oc.OrganizationId = o.OrganizationId
  AND o.RefOrganizationTypeId = 10
  GROUP BY oc.Organizationid
  ) oce_colegio
  ON oce_colegio.org = OrgSchool
-------------------------------------------------------------------------------------------------------------------------------------------------------------------
JOIN (
	SELECT OrganizationId as cssOrgId, group_concat(DISTINCT ClassMeetingDays) as cssClassMeetingDays
	FROM CourseSectionSchedule css
	GROUP BY organizationId
) md 
ON md.cssOrgId = Organizationid
-------------------------------------------------------------------------------------------------------------------------------------------------------------------
WHERE 
  CAST(strftime('%w',date) as INTEGER) between 1 and 5
  AND ifnull(oce.fechasEventos,'1900-01-01') NOT LIKE "%"  || date || "%"	 
  AND ifnull(occ.fechasCrisis,'1900-01-01') NOT LIKE "%"|| date || "%"
  AND ifnull(oce_colegio.fechasEventos,'1900-01-01')  NOT LIKE "%"|| date || "%"
  --AND result.idAsignatura NOT NULl
  AND md.cssClassMeetingDays like "%" || diaSemanaCalendar || "%"	   
GROUP BY Organizationid, date
""")
    except Exception as e:
        logger.error(f"Resultado: {str(e)}")

    # define listas de errores
    workDayWithoutInfo = []
    courseSectionNameErrors = []
    totalStudentsErrors = []
    tokenRegisteredErrors = []
    descriptionClassErrors = []
    try:
        if(not asignaturas):
            logger.error(f"S/Datos")
            logger.info(
                f'No hay información disponible para validar. Su registro es obligatorio.')
            logger.info(
                f'Si hay información en la BD, revise si esta cumpliendo con los criterios de la consulta.')
            raise ValueError(f"No hay informacion")

        for asignaturaRow in asignaturas:
            # define variables a comparar
            organizationId = asignaturaRow[0]
            workDay = asignaturaRow[1]
            CourseSectionId = asignaturaRow[2]
            courseSectionName = asignaturaRow[3]
            totalStudents = asignaturaRow[9]
            presentStudents = asignaturaRow[10] if (asignaturaRow[10]) else 0
            ausentStudents = asignaturaRow[12] if (asignaturaRow[12]) else 0
            Latestudent = asignaturaRow[14] if (asignaturaRow[14]) else 0
            tokenRegistered = asignaturaRow[16]
            descriptionClass = asignaturaRow[17]

            # Comienza a validar los datos
            if(not CourseSectionId):
                # se encontraron días hábiles del calendatio sin información registrada
                workDayWithoutInfo.append(workDay)

            else:  # Valida solo si existe información en la fecha
                if(not courseSectionName):
                    # Se encontraron asignaturas sin ningún nombre registrado
                    courseSectionNameErrors.append(organizationId)

                if(totalStudents != (presentStudents+ausentStudents+Latestudent)):
                    # La cantidad total de estudiantes no coincide con la suma de estudiantes presentes+ausentes+atradados.
                    totalStudentsErrors.append(organizationId)

                if(tokenRegistered != totalStudents):
                    # La cantidad de firmas registradas no coinciden con el número total de estudiantes
                    tokenRegisteredErrors.append(organizationId)

                if(not descriptionClass):
                    # La clase registrada en el día X, no contiene descripción de los temas trabajados (Leccionario)
                    descriptionClassErrors.append(organizationId)

        if(workDayWithoutInfo or courseSectionNameErrors or totalStudentsErrors or tokenRegisteredErrors or descriptionClassErrors):
            if(workDayWithoutInfo):
                logger.error(
                    f'Se encontraron días hábiles del calendario sin información registrada: {workDayWithoutInfo}')
            if(courseSectionNameErrors):
                logger.error(
                    f'Se encontraron asignaturas sin ningún nombre registrado: {courseSectionNameErrors}')
            if(totalStudentsErrors):
                logger.error(
                    f'La cantidad total de estudiantes no coincide con la suma de estudiantes presentes+ausentes+atradados en las siguientes asignaturas: {totalStudentsErrors}')
            if(tokenRegisteredErrors):
                logger.error(
                    f'La cantidad de firmas registradas no coinciden con el número total de estudiantes en las siguientes asignaturas: {tokenRegisteredErrors}')
            if(descriptionClassErrors):
                logger.error(
                    f'La clase registrada en el día X, no contiene descripción de los temas trabajados (Leccionario). {descriptionClassErrors}')
        else:
            _r = True
    except Exception as e:
        logger.error(
            f"Error on line {sys.exc_info()[-1].tb_lineno}, {type(e).__name__},{e}")
        logger.error(f"{str(e)}")
    finally:
        logger.info(f"Aprobado") if _r else logger.error(f"Rechazado")
        return_dict[getframeinfo(currentframe()).function] = _r
        logger.info(f"{current_process().name} finalizando...")
        return _r
