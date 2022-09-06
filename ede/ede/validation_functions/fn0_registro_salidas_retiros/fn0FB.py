from inspect import getframeinfo, currentframe
from multiprocessing import current_process

import ede.ede.check_utils as check_utils
from ede.ede._logger import logger


def fn0FB(conn, return_dict):
    """
    REGISTRO DE SALIDAS
    7.0 Registro de salidas o retiros (NO Habituales)
    Verificar, en caso que existan retiros anticipados, que se encuentre registrado
    el “verificador de identidad” o escaneado el poder simple o la comunicación 
    que autorice la salida del estudiante, según corresponda. Apodrado, papá, mamá, etc. 
    Se puede filtrar por RoleAttendanceEvent.RefAttendanceStatusID == 5 (Early Departure) 
    y agrupar por Date para obtener el bloque de registros      
    Args:
        conn ([sqlalchemy.engine.Connection]): [
          Objeto que establece la conexión con la base de datos.
          Creado previamente a través de la función execute(self)
          ]
    Returns:
        [Boolean]: [
          Retorna True y "S/Datos" a través de logger, si no existe información en el sistema.
          Retorna True y "Aprobado" a través de logger, si cada estudiante cumple con los siguientes criterios:
            - La firma del apoderado se encuentra registrada en el sistema. (ERROR)
            - La persona que retiró se encontraba autorizada para hacerlo en el sistema. (WARNING)
            - El registro de retiro del estudiante desde la sala de clases debe ser anterior al registro de salida del estableciento. (ERROR)
            - Todos los registros del roleAttendanceEvent deben estar firmados. (ERROR)
            - El tipo RoleAttendanceEvant.RefAttendanceStatusID debe ser == 5 (Early Departure). (ERROR)
            - En roleAttendanceEvent debe estar el campo observaciones con el detalle del motivo del retiro anticipado. (ERROR)
          En todo otro caso, retorna False y "Rechazado" a través de logger.
          ]
    """
    _r = False
    Allrows = []
    try:
        rows = conn.execute("""--sql
            SELECT rae.RoleAttendanceEventId
            FROM RoleAttendanceEvent rae
            -- Antes de realizar cualquier acción se revisa que el estudiante tengan
            -- registrada alguna salida anticipada
            JOIN RefAttendanceStatus ras
              ON ras.RefAttendanceStatusId = rae.RefAttendanceStatusId
              AND ras.Code IN ('EarlyDeparture')
      """).fetchall()
        Allrows = check_utils.convertirArray2DToList(
            list([m[0] for m in rows if m[0] is not None]))
    except Exception as e:
        logger.info(f"Resultado: {Allrows} -> {str(e)}")

    if(len(Allrows) == 0):
        logger.info(
            f"NO existen registros de retiro anticipado de alumnos en el establecimiento.")
        logger.info(f"S/Datos")
        return_dict[getframeinfo(currentframe()).function] = True
        logger.info(f"{current_process().name} finalizando...")
        return True  # si no hay registros de salida anticipada, no continúa la revisión
    try:
        if(len(Allrows) > 0):
            rows = conn.execute("""--sql
            SELECT 
                raeAlumnoAsignatura.RoleAttendanceEventId as 'RoleAttendanceEventIdAlumnoAsignatura'
                ,raeAlumnoEstablecimieto.RoleAttendanceEventId as 'RoleAttendanceEventIdAlumnoEstablecimiento'
                ,raeApoderado.RoleAttendanceEventId as 'AlumnoAsignaturaApoderado'
            --	, raeAlumnoEstablecimieto.*
            --	, raeApoderado.*

            FROM RoleAttendanceEvent raeAlumnoAsignatura

            -- Antes de realizar cualquier acción se revisa que el estudiante tenga
            -- Registrada una salida anticipada
            JOIN RefAttendanceStatus ras
              ON ras.RefAttendanceStatusId = raeAlumnoAsignatura.RefAttendanceStatusId
              AND ras.Code IN ('EarlyDeparture')
            -- Esta relación obliga que el registro sea de tipo asignatura en la tabla RoleAttendanceEvent
            JOIN RefAttendanceEventType raet
              ON raet.RefAttendanceEventTypeId IN (
                SELECT RefAttendanceEventTypeId
                FROM RefAttendanceEventType
                WHERE Code IN ('ClassSectionAttendance')
              )

            -- Establece la relación con la tabla OrganizationPersonRole
            JOIN OrganizationPersonRole oprAlumnoAsignatura
              ON oprAlumnoAsignatura.OrganizationPersonRoleId = raeAlumnoAsignatura.OrganizationPersonRoleId
              AND oprAlumnoAsignatura.RecordEndDateTime IS NULL

            -- Esta relación obliga que el registro sea hecho utilizando el rol de estudiante
            JOIN Role rol
              ON rol.RoleId = oprAlumnoAsignatura.RoleId
              AND rol.Name IN ('Estudiante')
            -- verifica que el rol se encuentre vigente
            JOIN RoleStatus rst
              ON rst.OrganizationPersonRoleId = oprAlumnoAsignatura.OrganizationPersonRoleId
              AND rst.StatusEndDate IS NULL
              AND rst.RecordEndDateTime IS NULL
              AND rst.RefRoleStatusId IN (
                SELECT RefRoleStatusId
                FROM RefRoleStatus
                WHERE code IN ('Enrolled')
              )
              
            -- Esta relación obliga al estudiante a estar asignado a una asignatura
            JOIN Organization asignatura
              ON asignatura.OrganizationId = oprAlumnoAsignatura.OrganizationId
              AND asignatura.RecordEndDateTime IS NULL
              AND asignatura.RefOrganizationTypeId IN (
                SELECT RefOrganizationTypeId
                FROM RefOrganizationType
                WHERE code IN ('CourseSection')
              )	
              
            -- Esta relación verifica que la asignatura no se encuentre asignada a un Nivel de Adultos
            JOIN OrganizationRelationship ors
              ON ors.OrganizationId = oprAlumnoAsignatura.OrganizationId
            JOIN jerarquiasList jer 
              ON ors.Parent_OrganizationId = jer.OrganizationIdDelCurso
              AND jer.nivel NOT IN ('03:Educación Básica Adultos'
                      ,'06:Educación Media Humanístico Científica Adultos'
                      ,'08:Educación Media Técnico Profesional y Artística, Adultos')

            ------------------------------------------------------------------------------------------
            -- Revisa que exista el registro de salida del alumno a nivel del establecimiento
            JOIN OrganizationPersonRole oprAlumnoEstablecimiento
              ON oprAlumnoEstablecimiento.personId = oprAlumnoAsignatura.personId
              AND oprAlumnoEstablecimiento.OrganizationId IN (
                -- Esta relación obliga al estudiante a estar asignado a un establecimiento
                SELECT Organizationid
                FROM Organization 
                WHERE 
                  Organization.OrganizationId = oprAlumnoEstablecimiento.OrganizationId
                  AND RecordEndDateTime IS NULL
                  AND RefOrganizationTypeId IN (
                    SELECT RefOrganizationTypeId
                    FROM RefOrganizationType
                    WHERE code IN ('K12School')
                  )		
              )

            JOIN RoleAttendanceEvent raeAlumnoEstablecimieto
              ON raeAlumnoEstablecimieto.OrganizationPersonRoleId = oprAlumnoEstablecimiento.OrganizationPersonRoleId
              AND raeAlumnoEstablecimieto.RefAttendanceStatusId IN (
                SELECT RefAttendanceStatusId
                FROM RefAttendanceStatus
                WHERE Code IN ('EarlyDeparture')	
              )
              AND raeAlumnoEstablecimieto.RefAttendanceEventTypeId IN (
                SELECT RefAttendanceEventTypeId
                FROM RefAttendanceEventType
                WHERE Code IN ('AsistenciaEstablecimiento')	
              )
              -- Verifica que los eventos ocurran el mismo día
              AND strftime('%Y-%m-%d',raeAlumnoEstablecimieto.Date) = strftime('%Y-%m-%d',raeAlumnoAsignatura.Date)	

            ------------------------------------------------------------------------------------------
                      

            --En PersonRelationship el campo personId identifica al apoderado y el campo RelatedPersonId al estudiante
            JOIN PersonRelationship prsh 
              ON oprAlumnoAsignatura.personId = prsh.RelatedPersonId
              AND prsh.RecordEndDateTime IS NULL
              AND prsh.RetirarEstudianteIndicador = 1 --Indica que se encuentra habilitado

            -- Verifica que el rol del la persona se encuentre dentro de las permitidas par retirara al estudiante
            JOIN OrganizationPersonRole oprApoderado
              ON oprApoderado.personId = prsh.personId
              AND oprApoderado.RoleId IN (
                SELECT RoleId
                FROM Role 
                WHERE Name IN ('Padre, madre o apoderado','Transportista','Persona que retira al estudiante')
              )
            -- Ahora relaciona el registro de oprApoderado con el de raeApoderado
            JOIN RoleAttendanceEvent raeApoderado
              ON raeApoderado.OrganizationPersonRoleId = oprApoderado.OrganizationPersonRoleId
              AND raeApoderado.RefAttendanceStatusId IN (
                SELECT RefAttendanceStatusId
                FROM RefAttendanceStatus
                WHERE Code IN ('EarlyDeparture')
              )
              AND raeApoderado.RefAttendanceEventTypeId IN (
                SELECT RefAttendanceEventTypeId
                FROM RefAttendanceEventType
                WHERE Code IN ('AsistenciaEstablecimiento')
              )	
              AND raeApoderado.RecordEndDateTime IS NULL
              -- Verifica que los eventos ocurran el mismo día
              AND strftime('%Y-%m-%d',raeApoderado.Date) = strftime('%Y-%m-%d',raeAlumnoAsignatura.Date)
              AND raeApoderado.observaciones IS NOT NULL
              AND raeApoderado.oprIdRatificador IS NOT NULL
              AND raeApoderado.firmaRatificador REGEXP '^[0-9]{6}([-]{1}[0-9kK]{1})?$'
              AND raeApoderado.fechaRatificador REGEXP '^(19|2[0-9])[0-9]{2}-(0[1-9]|1[012])-(0[1-9]|[12][0-9]|3[01])([T ])(0[0-9]|1[0-9]|2[0-3]):([0-5][0-9]):([0-5][0-9])(.[0-9]+)?((\+|\-)(0[0-9]|1[0-9]|2[0-3]):([0-5][0-9]))$'
              AND raeApoderado.Date  REGEXP '^(19|2[0-9])[0-9]{2}-(0[1-9]|1[012])-(0[1-9]|[12][0-9]|3[01])([T ])(0[0-9]|1[0-9]|2[0-3]):([0-5][0-9]):([0-5][0-9])(.[0-9]+)?((\+|\-)(0[0-9]|1[0-9]|2[0-3]):([0-5][0-9]))$'		
              
            WHERE 
              raeAlumnoAsignatura.observaciones IS NOT NULL
              AND
              raeAlumnoAsignatura.digitalRandomKey REGEXP '^[0-9]{6}([-]{1}[0-9kK]{1})?$'
              AND
              raeAlumnoAsignatura.digitalRandomKeyDate REGEXP '^(19|2[0-9])[0-9]{2}-(0[1-9]|1[012])-(0[1-9]|[12][0-9]|3[01])([T ])(0[0-9]|1[0-9]|2[0-3]):([0-5][0-9]):([0-5][0-9])(.[0-9]+)?((\+|\-)(0[0-9]|1[0-9]|2[0-3]):([0-5][0-9]))$'
              AND
              raeAlumnoEstablecimieto.digitalRandomKey REGEXP '^[0-9]{6}([-]{1}[0-9kK]{1})?$'
              AND
              raeAlumnoEstablecimieto.digitalRandomKeyDate REGEXP '^(19|2[0-9])[0-9]{2}-(0[1-9]|1[012])-(0[1-9]|[12][0-9]|3[01])([T ])(0[0-9]|1[0-9]|2[0-3]):([0-5][0-9]):([0-5][0-9])(.[0-9]+)?((\+|\-)(0[0-9]|1[0-9]|2[0-3]):([0-5][0-9]))$'
              AND
              raeAlumnoAsignatura.Date  REGEXP '^(19|2[0-9])[0-9]{2}-(0[1-9]|1[012])-(0[1-9]|[12][0-9]|3[01])([T ])(0[0-9]|1[0-9]|2[0-3]):([0-5][0-9]):([0-5][0-9])(.[0-9]+)?((\+|\-)(0[0-9]|1[0-9]|2[0-3]):([0-5][0-9]))$'
              AND
              raeAlumnoEstablecimieto.Date  REGEXP '^(19|2[0-9])[0-9]{2}-(0[1-9]|1[012])-(0[1-9]|[12][0-9]|3[01])([T ])(0[0-9]|1[0-9]|2[0-3]):([0-5][0-9]):([0-5][0-9])(.[0-9]+)?((\+|\-)(0[0-9]|1[0-9]|2[0-3]):([0-5][0-9]))$'
              AND
              (
                raeApoderado.digitalRandomKey REGEXP '^[0-9]{6}([-]{1}[0-9kK]{1})?$' AND raeApoderado.digitalRandomKeyDate REGEXP '^(19|2[0-9])[0-9]{2}-(0[1-9]|1[012])-(0[1-9]|[12][0-9]|3[01])([T ])(0[0-9]|1[0-9]|2[0-3]):([0-5][0-9]):([0-5][0-9])(.[0-9]+)?((\+|\-)(0[0-9]|1[0-9]|2[0-3]):([0-5][0-9]))$'
                OR 
                raeApoderado.fileScanBase64 IS NOT NULL
              )
              AND 
              --Verifica que el registro de salida de la sala de clases haya sido anterior a la salida del establecimiento
              strftime('%H:%M:%f',raeAlumnoAsignatura.Date) <= strftime('%H:%M:%f',raeAlumnoEstablecimieto.Date)
              AND 
              strftime('%H:%M:%f',raeAlumnoAsignatura.Date) <= strftime('%H:%M:%f',raeApoderado.Date)
        """).fetchall()
    except Exception as e:
        logger.info(f"Resultado: {rows} -> {str(e)}")
    try:
        if(len(rows) > 0):
            for row in rows:
                for i in range(3):
                    try:
                        Allrows.remove(row[i])
                    except:
                        pass

            if(len(Allrows) == 0):
                _r = True
            else:
                logger.info(
                    f"RoleAttendanceEventIdAlumnoAsignatura con problemas: {Allrows}")
        else:
            logger.error(f"Rechazado")
            logger.info(
                f"RoleAttendanceEventIdAlumnoAsignatura con problemas: {Allrows}")
    except Exception as e:
        logger.error(
            f"NO se pudo ejecutar la consulta de retiros anticipados: {str(e)}")
        logger.error(f"Rechazado")
    finally:
        return_dict[getframeinfo(currentframe()).function] = _r
        logger.info(f"{current_process().name} finalizando...")
        return _r
