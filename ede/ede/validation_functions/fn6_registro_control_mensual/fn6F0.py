from inspect import getframeinfo, currentframe
from multiprocessing import current_process
import sys

from ede.ede._logger import logger

def fn6F0(conn, return_dict):
    """
    REGISTRO CONTROL MENSUAL DE ASISTENCIA O CONTROL DE SUBVENCIONES
      6.2 Contenido mínimo, letra c
      Verificar que exista el registro de asistencia en aquellos casos en los cuales 
      se realizó la clase al estudiante.
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
    rows = []
    try:
      # select para listar todos los colegios de tabla organizacion
      rows = conn.execute("""
-- 6.2 Contenido mínimo, letra c
-- erificar que exista el registro de asistencia en aquellos casos en los cuales se realizó la clase al estudiante.
-- * día de clases
-- * mes respectivo
-- * hora pedagógica
-- * nombre del curso
-- * total de estudiantes presentes y ausentes
-- * Verificador de identidad del docente a cargo

-- Lee desde las organizaciones de tipo curso los campos 
-- FirstInstructionDate y LastInstructionDate y con esa información
-- crea una lista de días hábiles en los cuales deberían haber tenido clases
-- los estudiantes del establecimiento.
WITH RECURSIVE dates(Organizationid, date) AS (
  -------------------------------------------------------------------------------------------------------------------------------------------------------------------
  SELECT 
    DISTINCT O.Organizationid
    , FirstInstructionDate
  FROM Organization O
  JOIN RefOrganizationType rot
    ON O.RefOrganizationTypeId = rot.RefOrganizationTypeId
    AND O.RefOrganizationTypeId IN 
      (
        SELECT RefOrganizationTypeId 
        FROM RefOrganizationType
        WHERE Description IN ('Course')
      ) 
  JOIN OrganizationCalendar oc
    ON oc.OrganizationCalendarId = ocs.OrganizationCalendarId
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
	  Organizationid -- [idx 0]
	, date -- [idx 1]
	, result.idCurso -- [idx 2]
	, result.nombreCurso -- [idx 3]
	, result.fechaAsistencia -- [idx 4]
	, result.fecha -- [idx 5]
	, result.diaSemana -- [idx 6]
	, result.Mes -- [idx 7]
	, result.hora -- [idx 8]
	, result.totalEstudiantes -- [idx 9]
	, result.estudiantesPresentes -- [idx 10]
	, result.estudiantesPresentesNumLista -- [idx 11] 
	, result.estudiantesAusentes -- [idx 12]
	, result.estudiantesAusentesNumLista -- [idx 13]
	, result.cantidadRegistrosFirmados -- [idx 14]
	, (SELECT OrganizationId FROM Organization WHERE RefOrganizationTypeId = 10) as 'OrgSchool' -- [idx 15]
FROM dates 
-------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- con el OrganizationId se hace un cruce con la consulta que calcula los datos a validar
LEFT JOIN (
    -- Esta consulta rescata desde la RoleAttendanceEvent los días con asistencia y calcula 
    -- el total de estudiantes presentes y ausentes.
    -- Además revisa si exiten las firmas del docente en cada registro y si esta cargada la 
    -- información del leccionario
    SELECT 
    O.OrganizationId as 'idCurso'
    ,O.name as 'nombreCurso' 
    ,rae.Date as 'fechaAsistencia' -- fecha completa de la clase 
    ,strftime('%Y-%m-%d', rae.Date) as 'fecha' -- rescata solo la fecha desde rae.Date 
    ,CASE 
      WHEN strftime('%w', rae.Date) = '0' THEN 'Domingo'
      WHEN strftime('%w', rae.Date) = '1' THEN 'Lunes'
      WHEN strftime('%w', rae.Date) = '2' THEN 'Martes'
      WHEN strftime('%w', rae.Date) = '3' THEN 'Miércoles'
      WHEN strftime('%w', rae.Date) = '4' THEN 'Jueves'
      WHEN strftime('%w', rae.Date) = '5' THEN 'Viernes'
      WHEN strftime('%w', rae.Date) = '6' THEN 'Sabado'
    END as 'diaSemana' -- rescata solo el día de la semana desde rae.Date 
    ,CASE 
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
    END as 'Mes' -- rescata solo el mes desde rae.Date
    ,strftime('%H:%M', rae.Date, substr(rae.Date,length(rae.Date)-5,6)) as 'hora' -- rescata solo la hora desde rae.Date
    ,count(rae.RoleAttendanceEventId) as 'totalEstudiantes' -- Cantidad total de estudiantes 
    ,sum(CASE WHEN rae.refattendancestatusid IN (1) THEN 1 ELSE 0 END) as 'estudiantesPresentes' 
    ,group_concat(CASE WHEN rae.refattendancestatusid IN (1) THEN Identifier END) as 'estudiantesPresentesNumLista' 
    ,sum(CASE WHEN rae.refattendancestatusid IN (2,3) THEN 1 ELSE 0 END) as 'estudiantesAusentes' 
    ,group_concat(CASE WHEN rae.refattendancestatusid IN (2,3) THEN Identifier END) as 'estudiantesAusentesNumLista' 
    ,count(rae.digitalRandomKey) as 'cantidadRegistrosFirmados' 
    FROM Organization O
    JOIN RefOrganizationType rot
      ON O.RefOrganizationTypeId = rot.RefOrganizationTypeId
      AND O.RefOrganizationTypeId IN (
        SELECT RefOrganizationTypeId 
        FROM RefOrganizationType
        WHERE Description IN ('Course')
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

    WHERE 
        rae.digitalRandomKey NOT NULL
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
ON result.idCurso = OrganizationId
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
-- Rescata las fechas desde OrganizationCalendarEvent y las saca de la lista de días hábiles
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
-------------------------------------------------------------------------------------------------------------------------------------------------------------------
WHERE 
  CAST(strftime('%w',date) as INTEGER) between 1 and 5
  AND date NOT LIKE "%" || ifnull(oce.fechasEventos,'1900-01-01') || "%"	 
  AND date NOT LIKE "%" || ifnull(occ.fechasCrisis,'1900-01-01') || "%"
GROUP BY Organizationid, date
      """).fetchall()
    except Exception as e:
      logger.error(f"Resultado: {rows} -> {str(e)}")
   
    #define listas de errores
    workDayWithoutInfo = []
    courseNameErrors = []
    totalStudentsErrors = []
    tokenRegisteredErrors = []
    try:
      #logger.debug(f"rows: {rows}, _organizationId: {rows[0][0]}")
      if(not rows):
        logger.error(f"S/Datos")
        logger.info(f'No hay información disponible para validar. Su registro es obligatorio.')
        logger.info(f'Si hay información en la BD, revise si esta cumpliendo con los criterios de la consulta.')
        raise ValueError(f"No hay informacion")
      
      for row in rows:
        #define variables a comparar
        organizationId = row[0]
        workDay = row[1]
        CourseId = row[2]
        courseName = row[3]
        totalStudents = row[9]
        presentStudents = row[10] if (row[10]) else 0
        ausentStudents = row[12]  if (row[12]) else 0
        tokenRegistered = row[14]
        
        #Comienza a validar los datos
        if(not CourseId): 
          #se encontraron días hábiles del calendatio sin información registrada
          workDayWithoutInfo.append(workDay)

        else: #Valida solo si existe información en la fecha
          if(not courseName):
            #Se encontraron asignaturas sin ningún nombre registrado
            courseNameErrors.append(organizationId)

          if(totalStudents != (presentStudents+ausentStudents)):
            #La cantidad total de estudiantes no coincide con la suma de estudiantes presentes+ausentes+atradados.
            totalStudentsErrors.append(organizationId)
            
          if(tokenRegistered != totalStudents):
            #La cantidad de firmas registradas no coinciden con el número total de estudiantes
            tokenRegisteredErrors.append(organizationId)
                        
      if(workDayWithoutInfo or courseNameErrors or totalStudentsErrors or tokenRegisteredErrors): 
        if(workDayWithoutInfo): logger.error(f'Se encontraron días hábiles del calendario sin información registrada: {workDayWithoutInfo}')
        if(courseNameErrors): logger.error(f'Se encontraron cursos sin ningún nombre registrado: {courseNameErrors}')
        if(totalStudentsErrors): logger.error(f'La cantidad total de estudiantes no coincide con la suma de estudiantes presentes+ausentes en los siguientes cursos: {totalStudentsErrors}')
        if(tokenRegisteredErrors): logger.error(f'La cantidad de firmas registradas no coinciden con el número total de estudiantes en los siguientes cursos: {tokenRegisteredErrors}')
      else:
        _r = True
    except Exception as e:
      logger.error(f"Error on line {sys.exc_info()[-1].tb_lineno}, {type(e).__name__},{e}")      
      logger.error(f"{str(e)}")
    finally:
      logger.info(f"Aprobado") if _r else logger.error(f"Rechazado")
      return_dict[getframeinfo(currentframe()).function] = _r
      logger.info(f"{current_process().name} finalizando...")      
      return _r
### fin fn6F0 ###