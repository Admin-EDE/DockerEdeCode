from inspect import getframeinfo, currentframe
from multiprocessing import current_process
import sys


from ede.ede._logger import logger


def fn5E0(conn, return_dict):
    """
    validar el registro de asistencia bloque a bloque.
    Args:
        conn ([sqlalchemy.engine.Connection]): [
          Objeto que establece la conexión con la base de datos.
          Creado previamente a través de la función execute(self)
          ]
    Returns:
        [Boolean]: [
          Retorna True/False y "S/Datos" a través de logger, solo si puede:
            - Sin asistencia por bloque
          Retorna True y “Aprobado” a través de logger, solo si se puede: 
            - Que contenga sea de tipo Asignatura ('Course Section')
            - Que el rol del estudiante este asignado al registro ('Estudiante')
            - Que esten registrados los números de lista (pid.refPersonIdentificationSystemId) 
            de los estudiantes ausentes, atrasados y presentes.
            - Que este presente el verificador de identidad (rae.digitalRandomKey NOT NULL)
            de la persona que se encuentre trabajando con el estudiante.
            - Que se encuentre cargado el indicado de virtualidad (rae.VirtualIndicator).
            - Que se encuentre cargado la descripción de lo realizado en clases (ocs.Description NOT NULL)
            - Que la hora de la toma de asistencia se encuentre en el horario de clases
            (strftime('%H:%M', rae.Date) between ClassBeginningTime and ClassEndingTime) [Aquí no arroja error pero si una advertencia]
          En todo otro caso, retorna False y "Rechazado" a través de logger.
          ]
    """      
    _r = False
    _ExistData = []
    try:
        _ExistData = conn.execute("""
            SELECT DISTINCT 
              rae.Date, -- fecha completa de la clase
              strftime('%Y-%m-%d', rae.Date) as 'fecha', -- rescata solo la fecha desde rae.Date
              strftime('%H:%M', rae.Date, substr(rae.Date,length(rae.Date)-5,6)) as 'hora', -- rescata solo la hora desde rae.Date
              CASE 
                WHEN strftime('%w', rae.Date) = '0' THEN 'Domingo'
                WHEN strftime('%w', rae.Date) = '1' THEN 'Lunes'
                WHEN strftime('%w', rae.Date) = '2' THEN 'Martes'
                WHEN strftime('%w', rae.Date) = '3' THEN 'Miércoles'
                WHEN strftime('%w', rae.Date) = '4' THEN 'Jueves'
                WHEN strftime('%w', rae.Date) = '5' THEN 'Viernes'
                WHEN strftime('%w', rae.Date) = '6' THEN 'Sabado'
              END as 'diaSemana', -- rescata solo el dpia de la semana desde rae.Date
              count(refattendancestatusid) as 'totalEstudiantes', -- Cantidad total de estudiantes
              sum(CASE WHEN refattendancestatusid IN (1) THEN 1 ELSE 0 END) as 'estudiantesPresentes', 
              group_concat(CASE WHEN refattendancestatusid IN (1) THEN Identifier END) as 'estudiantesPresentesNumLista',
              sum(CASE WHEN refattendancestatusid IN (2,3) THEN 1 ELSE 0 END) as 'estudiantesAusentes',
              group_concat(CASE WHEN refattendancestatusid IN (2,3) THEN Identifier END) as 'estudiantesAusentesNumLista',	
              sum(CASE WHEN refattendancestatusid IN (4) THEN 1 ELSE 0 END) as 'estudiantesRetrasados',
              group_concat(CASE WHEN refattendancestatusid IN (4) THEN Identifier END) as 'estudiantesRetrasadosNumLista',
              count(rae.digitalRandomKey) as 'firmadoEnClases'
            FROM Organization O
              OUTER LEFT JOIN RefOrganizationType rot
                ON O.RefOrganizationTypeId = rot.RefOrganizationTypeId
                AND O.RefOrganizationTypeId IN (
                  SELECT RefOrganizationTypeId 
                  FROM RefOrganizationType
                  WHERE Description IN ('Course Section')
                ) 
              OUTER LEFT JOIN OrganizationPersonRole opr 
                ON O.OrganizationId = opr.OrganizationId
                AND opr.RecordEndDateTime IS NULL
              OUTER LEFT JOIN RoleAttendanceEvent rae
                ON opr.OrganizationPersonRoleId = rae.OrganizationPersonRoleId
                AND rae.RecordEndDateTime IS NULL
              OUTER LEFT JOIN PersonIdentifier pid
                ON opr.personId = pid.personId
                AND pid.refPersonIdentificationSystemId IN (
                  SELECT refPersonIdentificationSystemId
                  FROM refPersonIdentificationSystem
                  WHERE Code IN ('listNumber')
                )
                AND pid.RecordEndDateTime IS NULL
              OUTER LEFT JOIN Role rol
                ON opr.RoleId = rol.RoleId
                AND opr.RoleId IN (
                  SELECT RoleId
                  FROM Role
                  WHERE Name IN ('Estudiante')
                )
              OUTER LEFT JOIN OrganizationCalendar oc 
                ON O.OrganizationId = oc.OrganizationId
                AND oc.RecordEndDateTime IS NULL
              OUTER LEFT JOIN OrganizationCalendarSession ocs
                ON oc.OrganizationCalendarId = ocs.OrganizationCalendarId
                AND ocs.RecordEndDateTime IS NULL
              OUTER LEFT JOIN CourseSectionSchedule css
                ON O.OrganizationId = css.OrganizationId
                AND css.RecordEndDateTime IS NULL
            WHERE 
              Date is not NULL
            GROUP BY rae.Date         
        """).fetchall()
        if(not _ExistData):
          raise Exception("No hay registros de información")
    except Exception as e:
        logger.info(f"S/Datos")
        logger.info(f'Sin asistencia por bloque: {e}')
        return_dict[getframeinfo(currentframe()).function] = True
        return True
    try:
        asistencia = conn.execute("""
            /*
            6.2 Contenido mínimo, letra b.2 -> validar el registro de asistencia bloque a bloque
            Verifica:
            - Que contenga sea de tipo Asignatura ('Course Section')
            - Que el rol del estudiante este asignado al registro ('Estudiante')
            - Que esten registrados los números de lista (pid.refPersonIdentificationSystemId) 
            de los estudiantes ausentes, atrasados y presentes.
            - Que este presente el verificador de identidad (rae.digitalRandomKey NOT NULL)
            de la persona que se encuentre trabajando con el estudiante.
            - Que se encuentre cargado el indicado de virtualidad (rae.VirtualIndicator).
            - Que se encuentre cargado la descripción de lo realizado en clases (ocs.Description NOT NULL)
            - Que la hora de la toma de asistencia se encuentre en el horario de clases
            (strftime('%H:%M', rae.Date) between ClassBeginningTime and ClassEndingTime) [Aquí no arroja error pero si una advertencia]
            */
            SELECT DISTINCT 
              --group_concat(rae.Date),
              rae.Date, -- fecha completa de la clase
              strftime('%Y-%m-%d', rae.Date) as 'fecha', -- rescata solo la fecha desde rae.Date
              strftime('%H:%M', rae.Date, substr(rae.Date,length(rae.Date)-5,6)) as 'hora', -- rescata solo la hora desde rae.Date
              CASE 
                WHEN strftime('%w', rae.Date) = '0' THEN 'Domingo'
                WHEN strftime('%w', rae.Date) = '1' THEN 'Lunes'
                WHEN strftime('%w', rae.Date) = '2' THEN 'Martes'
                WHEN strftime('%w', rae.Date) = '3' THEN 'Miércoles'
                WHEN strftime('%w', rae.Date) = '4' THEN 'Jueves'
                WHEN strftime('%w', rae.Date) = '5' THEN 'Viernes'
                WHEN strftime('%w', rae.Date) = '6' THEN 'Sabado'
              END as 'diaSemana', -- rescata solo el dia de la semana desde rae.Date
              count(refattendancestatusid) as 'totalEstudiantes', -- Cantidad total de estudiantes
              sum(CASE WHEN refattendancestatusid IN (1) THEN 1 ELSE 0 END) as 'estudiantesPresentes', 
              group_concat(CASE WHEN refattendancestatusid IN (1) THEN Identifier END) as 'estudiantesPresentesNumLista',
              sum(CASE WHEN refattendancestatusid IN (2,3) THEN 1 ELSE 0 END) as 'estudiantesAusentes',
              group_concat(CASE WHEN refattendancestatusid IN (2,3) THEN Identifier END) as 'estudiantesAusentesNumLista',	
              sum(CASE WHEN refattendancestatusid IN (4) THEN 1 ELSE 0 END) as 'estudiantesRetrasados',
              group_concat(CASE WHEN refattendancestatusid IN (4) THEN Identifier END) as 'estudiantesRetrasadosNumLista',
              count(rae.digitalRandomKey) as 'firmadoEnClases'
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
              JOIN Role rol
                ON opr.RoleId = rol.RoleId
                AND opr.RoleId IN (
                  SELECT RoleId
                  FROM Role
                  WHERE Name IN ('Estudiante')
                )
              JOIN OrganizationCalendar oc 
                ON O.OrganizationId = oc.OrganizationId
                AND oc.RecordEndDateTime IS NULL
              JOIN OrganizationCalendarSession ocs
                ON oc.OrganizationCalendarId = ocs.OrganizationCalendarId
                AND ocs.RecordEndDateTime IS NULL
              JOIN CourseSectionSchedule css
                ON O.OrganizationId = css.OrganizationId
                AND css.RecordEndDateTime IS NULL
            --GROUP BY rae.Date

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
              
            GROUP BY rae.Date
        """).fetchall()
    except Exception as e:
        logger.error(f'Rechazado')
        logger.info(f'No cumple con los criterios de la consulta: {e}')
        return_dict[getframeinfo(currentframe()).function] = False
        logger.info(f"{current_process().name} finalizando...")
        return False
    try:
        if(len(asistencia)>0):
            totalEstudiantes = list([m[4] for m in asistencia if m[4] is not None])
            estudiantesPresentes = list([m[5] for m in asistencia if m[5] is not None])
            estudiantesAusentes = list([m[7] for m in asistencia if m[7] is not None])
            estudiantesRetrasados = list([m[9] for m in asistencia if m[9] is not None])
            firmadoEnClases = list([m[11] for m in asistencia if m[11] is not None])
            
            for idx_,el_ in enumerate(totalEstudiantes):
              if(el_ != (estudiantesPresentes[idx_]+estudiantesAusentes[idx_]+estudiantesRetrasados[idx_])):
                logger.info(f'Rechazado')
                logger.info(f'Total de estudiantes NO coincide con Presentes+Ausentes+Atrasados')
                return_dict[getframeinfo(currentframe()).function] = False
                logger.info(f"{current_process().name} finalizando...")
                return False    
                
              if(el_ != firmadoEnClases[idx_]):
                logger.info(f'Rechazado')
                logger.info(f'Total de estudiantes NO coincide con cantidad de firmas')
                return_dict[getframeinfo(currentframe()).function] = False
                logger.info(f"{current_process().name} finalizando...")
                return False
            
        logger.info("Aprobado")    
        return_dict[getframeinfo(currentframe()).function] = True
        logger.info(f"{current_process().name} finalizando...")
        return True
    except Exception as e:
      logger.error(f"Error on line {sys.exc_info()[-1].tb_lineno}, {type(e).__name__},{e}")
      logger.error(f"{str(e)}")
    finally:
      logger.info(f"Aprobado") if _r else logger.error(f"Rechazado")
      return_dict[getframeinfo(currentframe()).function] = _r
      logger.info(f"{current_process().name} finalizando...")      
      return _r